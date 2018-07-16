# coding : utf-8
from unittest import TestCase
from worker_service import Worker
from time import sleep
import socket
import pickle
import threading
import os
import const

PORT_NUMBER = 16000
MASTER_IP = '127.0.0.1'
TEMP_DIR = '.'
ENCODE = 'utf-8'
DEFAULT_RETRY_COUNT = 3
DUMMY_PID = b'1234'

class TestSocketMaster:
    def __del__(self):
        self.clientSock.close()
        self.serverSock.close()
        self.serverTestSock.close()

    def SetupClient(self):
        self.clientSock.connect((MASTER_IP, PORT_NUMBER))

    def SetupServer(self):
        self.serverSock.listen(20)
        self.serverTestSock, self.clientAdd = self.serverSock.accept()
        self.connectState = True

    def SetupSocket(self):
        # サーバー側ソケット生成
        self.serverSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serverSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.serverSock.bind((MASTER_IP, PORT_NUMBER))
        # クライアント側ソケット生成
        self.clientSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clientSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.connectState = False
        serverThread = \
            threading.Thread( \
                target=self.SetupServer)
        serverThread.start()
        clientThread = \
            threading.Thread( \
                target=self.SetupClient)
        clientThread.start()
        while not self.connectState:
            sleep(3)

class TestExecResist(TestCase):
    def RecvTestDataMock1(self, sock, addr, newDir, retryCount):
        self.recvTestDataCallCount += 1
        return (True, True)

    def RecvTestDataMock2(self, sock, addr, newDir, retryCount):
        self.recvTestDataCallCount += 1
        if (self.recvTestDataCallCount < 3):
            return (True, False)
        else:
            return (True, True)

    def RecvTestDataMock3(self, sock, addr, newDir, retryCount):
        raise Exception('Test Exception')
        return (True, True)

    def RecvTestDataMock4(self, sock, addr, newDir, retryCount):
        raise ValueError('Test Value Error Exception')
        return (True, True)

    def RecvTestDataMock5(self, sock, addr, newDir, retryCount):
        raise OSError('Test Value Error Exception')
        return (True, True)

    def ExecTaskMock(self):
        self.execTaskCalled += 1

    def ExecTaskMock2(self):
        self.execTaskCalled += 1
        sleep(10)

    def test_ExecResist(self):
        worker = Worker('dummy')
        sockMaster = TestSocketMaster()
        sockMaster.SetupSocket()

        # テスト用フォルダがあれば削除
        testMkDir = '%s_%s' % (MASTER_IP, DUMMY_PID.decode(ENCODE))
        if os.path.exists(testMkDir):
            os.rmdir(testMkDir)

        self.recvTestDataCallCount = 0
        self.execTaskCalled = 0
        worker.RecvTestData = self.RecvTestDataMock1
        worker.ExecTask = self.ExecTaskMock

        sock=sockMaster.clientSock

        # 初回呼び出し
        thread = \
            threading.Thread( \
                target=worker.ExecResist, \
                args=(sockMaster.serverTestSock, sockMaster.clientAdd,))
        thread.start()
        sock.send(DUMMY_PID)
        state=sock.recv(1024)
        self.assertEqual( const.SUCCESS_RECV, state )
        thread.join()
        self.assertEqual(1, self.recvTestDataCallCount)
        self.assertTrue(os.path.isdir(testMkDir))
        self.assertEqual(MASTER_IP, worker.masterList[0][1][0])
        self.assertEqual(1, len(worker.masterList))
        self.assertEqual(1, self.execTaskCalled)
        self.assertTrue(worker.execWork)
        worker.execWork = False

        # フォルダが存在する場合はエラー出力してファイル受信しない
        thread = \
            threading.Thread( \
                target=worker.ExecResist, \
                args=(sockMaster.serverTestSock, sockMaster.clientAdd,))
        thread.start()
        sock.send(DUMMY_PID)
        state=sock.recv(1024)
        self.assertEqual( const.SUCCESS_RECV, state )
        state=sock.recv(1024)
        self.assertEqual(const.INIT_ERROR, state)
        thread.join()
        self.assertEqual(1, self.recvTestDataCallCount)
        # フォルダを消してないか確認
        self.assertTrue(os.path.isdir(testMkDir))
        os.rmdir(testMkDir)
        # タスクを実行しないか確認
        self.assertEqual(1, self.execTaskCalled)

        # RecvTestData() を連続して呼び出すか確認
        worker.RecvTestData = self.RecvTestDataMock2
        thread = \
            threading.Thread( \
                target=worker.ExecResist, \
                args=(sockMaster.serverTestSock, sockMaster.clientAdd,))
        thread.start()
        sock.send(DUMMY_PID)
        state=sock.recv(1024)
        self.assertEqual( const.SUCCESS_RECV, state )
        thread.join()
        # フォルダ確認
        self.assertTrue(os.path.isdir(testMkDir))
        os.rmdir(testMkDir)
        # スレッドの実行確認
        self.assertEqual(2, self.execTaskCalled)
        worker.execWork = False

        # エラー発生時にフォルダを消すか確認
        # 継続して呼び出すか確認
        worker.RecvTestData = self.RecvTestDataMock3
        thread = \
            threading.Thread( \
                target=worker.ExecResist, \
                args=(sockMaster.serverTestSock, sockMaster.clientAdd,))
        thread.start()
        sock.send(DUMMY_PID)
        state=sock.recv(1024)
        self.assertEqual( const.SUCCESS_RECV, state )
        state=sock.recv(1024)
        self.assertEqual(const.INIT_ERROR, state)
        thread.join()
        # フォルダ確認
        self.assertFalse(os.path.isdir(testMkDir))

        worker.RecvTestData = self.RecvTestDataMock4
        thread = \
            threading.Thread( \
                target=worker.ExecResist, \
                args=(sockMaster.serverTestSock, sockMaster.clientAdd,))
        thread.start()
        sock.send(DUMMY_PID)
        state=sock.recv(1024)
        self.assertEqual( const.SUCCESS_RECV, state )
        state=sock.recv(1024)
        self.assertEqual(const.INIT_ERROR, state)
        thread.join()
        # フォルダ確認
        self.assertFalse(os.path.isdir(testMkDir))

        worker.RecvTestData = self.RecvTestDataMock5
        thread = \
            threading.Thread( \
                target=worker.ExecResist, \
                args=(sockMaster.serverTestSock, sockMaster.clientAdd,))
        thread.start()
        sock.send(DUMMY_PID)
        state=sock.recv(1024)
        self.assertEqual( const.SUCCESS_RECV, state )
        state=sock.recv(1024)
        self.assertEqual(const.INIT_ERROR, state)
        thread.join()
        # フォルダ確認
        self.assertFalse(os.path.isdir(testMkDir))

class TestWorker(TestCase):
    def TaskSend(self, opt ):
        print( 'Send New Task' )
        self.clientSock.send( pickle.dumps((const.NEW_TASK, opt)) )
        # データサイズ, 結果の順で飛んでくる
        risSize=pickle.loads(self.clientSock.recv(1024))
        self.assertEqual(const.TRANS_SIZE,risSize[0])
        risSize=risSize[1]
        result=b''
        tmp=0
        print('Recv size:' + str(risSize) )
        while tmp < risSize:
            print('Recv st' )
            result += self.clientSock.recv(1024)
            print('Recv ed' )
            tmp += len(result)
        result = pickle.loads(result)
        self.assertEqual( const.TASK_RESULT, result[0])
        self.assertEqual( 0, result[1] )
        self.assertEqual( opt, result[2].decode(ENCODE).strip() )
        print('End TaskSend')

    def TaskSender(self):
        print('Send start info')
        self.clientSock.send(const.TASK_START)
        # タスク送信
        print( 'Task1' )
        self.TaskSend( 'Task1' )
        print( 'Task2' )
        self.TaskSend( 'Task2' )
        print( 'End' )
        # タスク終了
        self.clientSock.send( pickle.dumps((const.END_TASK, '')) )

    def EndTask(self, sock, realPath, virtPath):
        return 0

    def test___ExecTask(self):
        worker = Worker('dummy.bat')
        worker.EndTask = self.EndTask
        sockMaster = TestSocketMaster()
        sockMaster.SetupSocket()

        self.clientSock=sockMaster.clientSock

        clientThread = \
            threading.Thread( \
                target=self.TaskSender)
        clientThread.start()
        # ステータス受信
        worker._ExecTask(sockMaster.serverTestSock,(MASTER_IP, PORT_NUMBER), 'dummy')
        clientThread.join()

    def test_ExecTask(self):
        worker = Worker('dummy.bat')
        worker.EndTask = self.EndTask
        sockMaster = TestSocketMaster()
        sockMaster.SetupSocket()

        self.clientSock=sockMaster.clientSock
        worker.masterList.append( (sockMaster.serverTestSock, (MASTER_IP, PORT_NUMBER), 'dummy') )

        clientThread = \
            threading.Thread( \
                target=self.TaskSender)
        clientThread.start()
        # ステータス受信
        worker.ExecTask()
        clientThread.join()
