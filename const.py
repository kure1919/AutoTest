import sys

class _const:
    def __init__(self):
        self.DEFAULT_RETRY_COUNT=3
        self.ENCODE='utf-8'
        self.TRANS_FILES = b'1'
        self.END_TRANS = b'2'
        self.SUCCESS_RECV = b'3'
        self.FAIL_RECV = b'4'
        self.END_FILE = b'5'
        self.TASK_READY=b'6'
        self.NEW_TASK=b'7'
        self.END_TASK=b'8'
        self.TASK_RESULT=b'9'
        self.FAIL_TASK=b'10'
        self.TASK_START=b'11'
        self.TRANS_SIZE=b'12'
        self.INIT_ERROR=b'13'
        self.INIT_SUCCESS=b'14'
        self.GET_STATE=b'15'
        self.SUCCESS_CLOSE=b'14'

    class ConstError(TypeError):
        pass

    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise self.ConstError("Can't rebind const (%s)" % name)
        self.__dict__[name] = value

sys.modules[__name__]=_const()
