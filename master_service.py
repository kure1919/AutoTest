# -*- coding:utf-8 -*-
import socket
import pickle
import os
import const
import threading
from FileTransport import FileTransporter

from logging import getLogger, StreamHandler, DEBUG, CRITICAL
logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(CRITICAL)
logger.setLevel(CRITICAL)
logger.addHandler(handler)
logger.propagate = False

HOST = "192.168.10.104"
PORT = 16000
ENCODE = 'utf-8'

Workers = [ [HOST, PORT],]
SetUpFiles = [ 'ope_test', ]

class Master:
    def __init__(self, baseDir):
        self.fileTrans=FileTransporter()
        self.workerList=[]
        self.baseDir=baseDir
        self.lock = threading.Lock()

    def _MasterRegist( self, workers, retryCount ):
        errorList=[]
        currentResist=None
        for worker in workers:
            if worker is None:
                continue
            try:
                currentResist=worker
                host = worker[0]
                port = worker[1]

                # プロセス番号通知
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                addr=(host, port)
                client.connect(addr)
                pid=str(os.getpid()).encode(const.ENCODE)
                logger.debug('Send pid(%s) info to %s' % (pid,addr[0]) )
                client.send( pid )
                state=client.recv(1024)
                if const.SUCCESS_RECV != state:
                    raise ValueError('Recv error state on init')
                # 検査用のファイル群を送信
                for elem in SetUpFiles:
                    file='%s/%s' % (self.baseDir,elem)
                    logger.debug('Send dirs( %s ) to %s' % (file,addr[0]) )
                    self.fileTrans.RecurseSendData(client, addr, file, elem)
                # 終了の通知
                self.fileTrans.SendEndInfo(client, addr)
                # ワーカーリストに登録
                self.workerList.append([client, addr])
            except OSError as e:
                print( e.strerror )
                errorList.append( currentResist )
            except ValueError as e:
                print( e )
                errorList.append( currentResist )

        if( 0 < retryCount ):
            self._MasterRegist( errorList, retryCount - 1 )

        return 0 < len(self.workerList)

    def __GetTaskList(self, taskFile):
        taskList=[]
        with open(taskFile, "r") as f:
            for line in f:
                line = line.strip()
                if '#' == line:
                    continue
                if 0 == len( line ):
                    continue
                taskList.append(line)
        return taskList

    def __GetNewTask(self):
        task=None
        with self.lock:
            if 0 < len( self.taskList ):
                task = self.taskList.pop(0)
        return task

    def __WorkerCloseTask(self, sock, addr):
        sock.send( pickle.dumps([const.END_TASK,'']) )
        recvDir='%s/ope_test/result'%(self.baseDir)
        while True:
            logger.debug('Exec data copy( recv )')
            result, endFlag = self.fileTrans.RecvData(sock, addr, recvDir, const.DEFAULT_RETRY_COUNT)
            if endFlag:
                break;
        # send で連続してパケットが飛ばさないように recv を入れる
        sock.recv(1024)
        sock.send(const.SUCCESS_CLOSE)
        sock.close()

    def __SendTaskWorkerUnit(self, sock, addr):
        try:
            logger.debug('Send start info to %s' % (addr[0]) )
            sock.send(const.TASK_START)
            state=sock.recv(1024)
            if const.TASK_READY != state:
                raise ValueError('Error worker state.')

            logger.debug('Get new task to %s' % (addr[0]) )
            task=self.__GetNewTask()

            ret=True
            if task is None:
                logger.debug('No task close(%s)' % (addr[0]) )
                sock.send( pickle.dumps([const.END_TASK,'']) )
            else:
                logger.debug('Send task(%s) to %s' % (task, addr[0]) )
                sock.send( pickle.dumps([const.NEW_TASK, task]) );

                while True:
                    logger.debug('Recv result size from %s' % (addr[0]))
                    data=sock.recv(1024)
                    if 0 == len( data ):
                        logger.debug('Recv fail retry.')
                        continue
                    data = pickle.loads( data );
                    if const.TRANS_SIZE == data[0]:
                        logger.debug('Send recv state')
                        sock.send(const.SUCCESS_RECV)
                        resultSize=data[1]
                        result = b''
                        tmp = 0
                        logger.debug('Recv result size:' + str(resultSize))
                        while tmp < resultSize:
                            data = sock.recv(1024)
                            tmp += len(data)
                            result += data
                        logger.debug('Recv result end')
                        result = pickle.loads(result)
                        if result[1] == 0:
                            print( 'Job %s is OK' % (result[2]))
                        else:
                            print( 'Job %s is NG' % (result[2]))
                            print( result[3].decode('sjis', errors="ignore") )
                            ret = False

                        logger.debug('Get new task to %s' % (addr[0]) )
                        task=self.__GetNewTask()
                        if not task is None:
                            logger.debug('Send new task(%s) to %s' % (task,addr[0]) )
                            taskInfo = pickle.dumps([const.NEW_TASK, task])
                            sock.send( taskInfo )
                        else:
                            break
                    else:
                        logger.debug('Fail recv msg')
                        logger.debug(data[0])
                        logger.debug(data[1])
                        sock.send(const.FAIL_RECV)
                        break
            self.__WorkerCloseTask(sock,addr)

            if not ret:
                self.taskResult = False
        except:
            import traceback
            traceback.print_exc()
            logger.debug('Set fail state(%s)' % (addr[0]) )
            self.taskResult = False

    def _SendTask(self, taskFile):
        self.taskList=self.__GetTaskList(taskFile)
        self.taskResult=True

        threadList=[]
        for worker in self.workerList:
            workThread = \
                threading.Thread( \
                    target=self.__SendTaskWorkerUnit, \
                    args=(worker[0],worker[1]))
            workThread.start()
            threadList.append( workThread )

        for thread in threadList:
            thread.join()

        return self.taskResult

    def ExecTask(self):
        if self._MasterRegist(Workers,const.DEFAULT_RETRY_COUNT):
            return self._SendTask('%s/ope_test/case/task_list.txt' % (self.baseDir) )
        else:
            return False;

if __name__ == "__main__":
    master = Master('.');
    exit( master.ExecTask() )
