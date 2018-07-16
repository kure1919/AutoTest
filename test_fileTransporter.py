from unittest import TestCase
from FileTransport import FileTransporter
import os
import pickle
import const

class DummySocket1(TestCase):
    def __init__(self):
        self.recvCallCount = 0
        self.sendCallCount = 0

    def recv(self, buffSize):
        if self.recvCallCount == 0:
            return pickle.dumps((const.TRANS_FILES, 'dummy_path', True))

        self.recvCallCount += 1

    def send(self, value):
        if self.sendCallCount == 0:
            if const.SUCCESS_RECV != value:
                raise Exception('Fail info protocol failed.')

class TestWorker(TestCase):
    def test_RecvTestData(self):
        trans=FileTransporter()
        try:
            if os.path.exists('dummy_path'):
                os.rmdir('dummy_path')
            result, endFlag = trans.RecvData(DummySocket1(), ('127.0.0.1','0'), './', 0);
            self.assertTrue(result)
            self.assertFalse(endFlag)
            self.assertTrue(os.path.isdir('dummy_path'))
            os.rmdir('dummy_path')
        except Exception:
            import traceback
            traceback.print_exc()

class TestFileTransporter(TestCase):
    def test_RecvTestData(self):
        self.fail()
