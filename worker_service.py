# coding : utf-8

import socket
import pickle
import threading
import shutil
import os
import subprocess
from FileTransport import FileTransporter
import const

from logging import getLogger, StreamHandler, DEBUG
logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(DEBUG)
logger.addHandler(handler)
logger.propagate = False

PORT_NUMBER = 16000
MASTER_IP = '192.168.10.50'
TEMP_DIR = '.'
ENCODE = 'utf-8'

class Worker:
    def __init__(self, workCmd):
        self.workCmd=workCmd
        self.masterList=[]
        self.execWork=False
        self.lock = threading.Lock()
        self.trans = FileTransporter()

    def RecvTestData(self, sock, addr, baseDir, retryCount):
        return self.trans.RecvData(sock, addr, baseDir, retryCount)

    def EndTask(self, sock, addr, tempDir):
        realPath='%s/ope_test/result' % (tempDir)
        self.trans.RecurseSendData(sock, addr, realPath, '')
        self.trans.SendEndInfo(sock, addr)
        logger.debug('Send get state msg to %s' % (addr[0]))
        # send で連続してパケットが飛んでこないようにここで send を入れる
        sock.send(const.GET_STATE)
        recv = sock.recv(1024);
        if const.SUCCESS_CLOSE == recv:
            shutil.rmtree( tempDir );

    def ExecCmd(self, cmd):
        p=subprocess.Popen( cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdOut, stdErr = p.communicate()
        return ( p.returncode, stdOut + stdErr )

    def _ExecTask(self, sock, addr, tempDir):
        while True:
            try:
                # タスク準備完了を通知
                logger.debug('Wait start info %s' % (addr[0]))
                status=sock.recv(1024)
                if const.TASK_START == status:
                    break;
                else:
                    logger.debug('Fail(%s) could not start' % (addr[0]))
                    return -1
            except socket.timeout:
                logger.debug('Time out %s, retry.' % (addr[0]))
            except:
                import traceback
                traceback.print_exc()
                logger.debug('Send fail info to %s' % (addr[0]))
                sock.send( pickle.dumps([const.FAIL_TASK, tempDir]) )
                sock.shutdown()
                return -1

        runTask=False
        try:
            logger.debug('Send task ready to %s' % (addr[0]))
            sock.send(const.TASK_READY)
            while True:
                logger.debug('Recv new task')
                data=sock.recv(1024)
                # ( モード, パス, ディレクトリか否か ) の配列が飛んでくる
                data = pickle.loads(data)
                mode = data[0]
                task = data[1]
                if const.NEW_TASK == mode:
                    cmdStr='%s %s' % (self.workCmd, task)
                    logger.debug('Exec cmd: %s' % (cmdStr))
                    ret, output = self.ExecCmd(cmdStr )
                    result  = pickle.dumps( [const.TASK_RESULT, ret, task, output] )
                    resSize = pickle.dumps( [const.TRANS_SIZE, len(result)] )
                    logger.debug('Send result size to %s, %d' % (addr[0], len(result)))
                    sock.send(resSize)
                    logger.debug('Recv send result size state from %s' % (addr[0]))
                    state=sock.recv(1024)
                    if const.SUCCESS_RECV != state:
                        raise ValueError('Faile send task result size')
                    logger.debug('Send result to %s' % (addr[0]))
                    sock.send(result)
                    logger.debug('End send to %s' % (addr[0]))
                    runTask=True
                elif const.END_TASK == mode:
                    logger.debug('Recv EndTask from %s' % (addr[0]))
                    break
                else:
                    logger.debug('Error unknown %s, %s' % (addr[0], tempDir))
                    break

        except ValueError as e:
            print('Error : {0}'.format(e))
        except Exception:
            import traceback
            traceback.print_exc()
            logger.debug('Send fail info to %s' % (addr[0]))
            sock.send( pickle.dumps([const.FAIL_TASK, tempDir]) )

        # 結果を送信
        if runTask:
            self.EndTask( sock, addr, tempDir )

    def ExecTask(self):
        # タスク処理用データを取得
        while 0 < len( self.masterList ):
            with self.lock:
                masterSock, masterAddr, tempDir=self.masterList.pop(0)

            try:
                self._ExecTask(masterSock,masterAddr,tempDir)
            except:
                import traceback
                traceback.print_exc()
            finally:
                logger.debug('Close socket(%s)' % (masterAddr[0]))
                masterSock.close()

        with self.lock:
            logger.debug('Change exec state')
            self.execWork = False

    # マスター登録処理
    def ExecResist(self, sock, addr):
        # プロセス ID 取得
        newDir = ''
        try:
            logger.debug('Recv ip from %s' % (addr[0]) )
            pid = sock.recv(1024).decode(ENCODE);
            sock.send(const.SUCCESS_RECV)
            newDir = '{}/{}_{}'.format(TEMP_DIR, addr[0], pid)
            logger.debug('Make dir %s' % (newDir) )
            os.makedirs(newDir);
            while True:
                logger.debug('Exec data copy( recv ) from %s' % (addr[0]) )
                result, endFlag = self.RecvTestData(sock, addr, newDir, const.DEFAULT_RETRY_COUNT)
                if endFlag:
                    break;
            if result:
                # マスターを登録
                with self.lock:
                    logger.debug('Append master( %s )' % (addr[0]) )
                    self.masterList.append( [ sock, addr, newDir ] )
                    if not self.execWork:
                        logger.debug('Exec task( %s )' % (addr[0]) )
                        self.execWork = True
                        workThread = \
                            threading.Thread( \
                                target=self.ExecTask)
                        workThread.start()
        except FileExistsError:
            import traceback
            traceback.print_exc()
            sock.send(const.INIT_ERROR);
        except OSError as e:
            print('Error : {0}'.format(e.strerror))
            sock.send(const.INIT_ERROR);
            if (0 < len(newDir)):
                shutil.rmtree(newDir);
        except ValueError as e:
            print('Error : {0}'.format(e))
            sock.send(const.INIT_ERROR);
            if (0 < len(newDir)):
                shutil.rmtree(newDir);
        except Exception:
            import traceback
            traceback.print_exc()
            sock.send(const.INIT_ERROR);
            if (0 < len(newDir)):
                shutil.rmtree(newDir);

    def MasterResist(self):
        try:
            resistSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            resistSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            resistSock.bind((MASTER_IP, PORT_NUMBER))
            while True:
                resistSock.listen(20)
                newSocket, newMasAdd = resistSock.accept()
                resistThread = \
                    threading.Thread( \
                        target=self.ExecResist, args=(newSocket, newMasAdd,))
                resistThread.start()

        except OSError as e:
            print('Error : {0}'.format(e.strerror))

if __name__ == "__main__":
    worker=Worker('.\\dummy.bat')
    masterResist = threading.Thread(target=worker.MasterResist)
    masterResist.start()
