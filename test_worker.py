# coding : utf-8
from unittest import TestCase
from worker_service import Worker
from time import sleep
import socket
import pickle
import threading
import os

TRANS_FILES = b'1'
END_TRANS = b'2'
SUCCESS_RECV = b'3'
FAIL_RECT = b'4'
END_FILE = b'5'

PORT_NUMBER = 16000
MASTER_IP = '127.0.0.1'
TEMP_DIR = '.'
ENCODE = 'utf-8'
DEFAULT_RETRY_COUNT = 3
DUMMY_PID = b'1234'


class TestDataSender:
    def _TestCase1(self, sock):
        sock.send(DUMMY_PID);

    def TestCase1(self, sock):
        clientThread = \
            threading.Thread( \
                target=self._TestCase1, args=(sock,))
        clientThread.start()


class TestExecResist(TestCase):
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

    def RecvTestDataMock1(self, newDir, retryCount):
        self.recvTestDataCallCount += 1
        return (True, True)

    def RecvTestDataMock2(self, newDir, retryCount):
        self.recvTestDataCallCount += 1
        if (self.recvTestDataCallCount < 3):
            return (True, False)
        else:
            return (True, True)

    def RecvTestDataMock3(self, newDir, retryCount):
        raise Exception('Test Exception')
        return (True, True)

    def RecvTestDataMock4(self, newDir, retryCount):
        raise ValueError('Test Value Error Exception')
        return (True, True)

    def RecvTestDataMock5(self, newDir, retryCount):
        raise OSError('Test Value Error Exception')
        return (True, True)

    def test_ExecResist(self):
        worker = Worker()
        self.SetupSocket()
        while not self.connectState:
            sleep(3)

        # テスト用フォルダがあれば削除
        testMkDir = '%s_%s' % (MASTER_IP, DUMMY_PID.decode(ENCODE))
        if os.path.exists(testMkDir):
            os.rmdir(testMkDir)

        self.recvTestDataCallCount = 0
        worker.RecvTestData = self.RecvTestDataMock1
        worker.socket = self.serverTestSock

        sender = TestDataSender()
        sender.TestCase1(self.clientSock)
        worker.ExecResist(self.clientAdd)
        self.assertEqual(1, self.recvTestDataCallCount)
        self.assertTrue(os.path.isdir(testMkDir))

        # フォルダが存在する場合はエラー出力してファイル受信しない
        sender.TestCase1(self.clientSock)
        worker.ExecResist(self.clientAdd);
        self.assertEqual(1, self.recvTestDataCallCount)
        # フォルダを消してないか確認
        self.assertTrue(os.path.isdir(testMkDir))
        os.rmdir(testMkDir)

        # 継続して呼び出すか確認
        worker.RecvTestData = self.RecvTestDataMock2
        sender.TestCase1(self.clientSock)
        worker.ExecResist(self.clientAdd)
        self.assertEqual(3, self.recvTestDataCallCount)
        # フォルダ確認
        self.assertTrue(os.path.isdir(testMkDir))
        os.rmdir(testMkDir)

        # エラー発生時にフォルダを消すか確認
        # 継続して呼び出すか確認
        worker.RecvTestData = self.RecvTestDataMock3
        sender.TestCase1(self.clientSock)
        worker.ExecResist(self.clientAdd)
        # フォルダ確認
        self.assertFalse(os.path.isdir(testMkDir))

        worker.RecvTestData = self.RecvTestDataMock4
        sender.TestCase1(self.clientSock)
        worker.ExecResist(self.clientAdd)
        # フォルダ確認
        self.assertFalse(os.path.isdir(testMkDir))

        worker.RecvTestData = self.RecvTestDataMock5
        sender.TestCase1(self.clientSock)
        worker.ExecResist(self.clientAdd)
        # フォルダ確認
        self.assertFalse(os.path.isdir(testMkDir))

        # 後始末
        self.serverTestSock.close()
        self.serverSock.close()
        self.clientSock.close()

class DummySocket(TestCase):
    def __init__(self):
        self.recvCallCount=0
        self.sendCallCount=0

    def recv(self, buffSize):
        if self.recvCallCount == 0:
            return pickle.dumps( (TRANS_FILES, 'dymmy_path', True ) )

        self.recvCallCount += 1

    def send(self, value):
        if self.sendCallCount == 0:
            if SUCCESS_RECV != value:
                raise Exception('Fail info protocol failed.')

class TestWorker(TestCase):
    def test_RecvTestData(self):
        worker = Worker()
        worker.socket = DummySocket()
        try:
            result, endFlag = worker.RecvTestData( './', 0 );
            self.assertTrue( result )
            self.assertFalse( endFlag )
        except Exception:
            import traceback
            traceback.print_exc()
