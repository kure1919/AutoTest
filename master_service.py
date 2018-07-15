# -*- coding:utf-8 -*-
import socket
import pickle
import threading
import os

HOST = "192.168.10.50"
PORT = 16000
DEFAULT_RETRY_COUNT=3

TRANS_FILES=b'1'
END_TRANS=b'2'
SUCCESS_RECV=b'3'
FAIL_RECV=b'4'
END_FILE=b'5'

Workers = ( (HOST, PORT), )

class Master:
    def TransforTestData( self, path, socket, retryCount ):
        result = False
        print( 'Send : ' + path )
        isDir=os.path.isdir(path);
        try:
            # ファイル情報を送信
            data=pickle.dumps((TRANS_FILES, path, isDir))
            socket.send(data)
            status=socket.recv(1024)
            if SUCCESS_RECV != status:
                raise 'Fail send metadata'

            # ディレクトリでない場合はファイルデータを送信
            if not isDir:
                # 最初に転送するサイズを送信
                fileSize=os.path.getsize( path );
                socket.send(str( fileSize ).encode());

                f = open( path, 'rb' )
                sendData = f.read( 1024 )
                while 0 < len( sendData ):
                    socket.send( sendData )
                    sendData = f.read( 1024 )
                status=socket.recv(1024)
                if SUCCESS_RECV != status:
                    raise 'Fail send file'
            result = True
        except OSError as e:
            print( e.strerror )
        except ValueError as e:
            print( e )

        # リトライ
        if not result and ( 0 < retryCount ):
            print( 'Retry')
            result = TransforTestData( path, socket, retryCount - 1 )

        return  result

    def MasterRegist( self, workers, retryCount ):
        errorList=()
        currentResist=None
        for worker in workers:
            try:
                currentResist=worker
                host = worker[0]
                port = worker[1]

                # プロセス番号通知
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect((host, port))
                client.send( str(os.getpid()).encode() )
                # 検査用のファイル群を送信
                self.TransforTestData('venv', client, DEFAULT_RETRY_COUNT)
                self.TransforTestData('test.py', client, DEFAULT_RETRY_COUNT)
                self.TransforTestData('akbs_0010.zip', client, DEFAULT_RETRY_COUNT)

                # 終了の通知
                data=pickle.dumps((END_TRANS, '', False))
                client.send(data)
                status=client.recv(1024)
                if SUCCESS_RECV != status:
                    raise 'Fail send metadata'
            except OSError as e:
                print( e.strerror )
                errorList.add( currentResist )

        if( 0 < retryCount ):
            MasterRegist( errorList, retryCount - 1 )

if __name__ == "__main__":
    master = Master();
    masterResist = threading.Thread( target=master.MasterRegist, args=(Workers,DEFAULT_RETRY_COUNT,)  )
    masterResist.start()