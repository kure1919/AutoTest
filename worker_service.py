# coding : utf-8

import socket
import pickle
import threading
import shutil
import os

PORT_NUMBER=16000
MASTER_IP='192.168.10.50'
TEMP_DIR='.'
ENCODE='utf-8'
DEFAULT_RETRY_COUNT=3

TRANS_FILES=b'1'
END_TRANS=b'2'
SUCCESS_RECV=b'3'
FAIL_RECT=b'4'
END_FILE=b'5'

def RecvTestData( baseDir, socket, retryCount ):
    result = False
    endFlag=False
    try:
        print( 'Recv command' )
        # ファイル情報を受信
        data=socket.recv(1024)
        # ( モード, パス, ディレクトリか否か ) の配列が飛んでくる
        data=pickle.loads( data )
        mode=data[0]
        path=data[1]
        isDir=data[2]

        # 成功ステータスを返却
        socket.send( SUCCESS_RECV )
        if TRANS_FILES == mode:
            print( 'Recv : ' + path )
            fullPath='{}/{}'.format(baseDir,path )
            if isDir:
                # ディレクトリの場合はディレクトリを作成
                os.mkdir( fullPath )
            else:
                # ディレクトリでない場合はファイルデータを受信
                f=open(fullPath,'wb')
                tmp=b''
                while True:
                    tmp=socket.recv(1024)
                    if tmp == END_FILE:
                        break;
                    else:
                        f.write(tmp)
                socket.send( SUCCESS_RECV )
            result = True
        elif END_TRANS == mode:
            socket.send( SUCCESS_RECV )
            endFlag=True
            result = True
        else:
            raise ValueError('mod is invalid.')
    except OSError as e:
        socket.send(FAIL_RECV)
        print( e.strerror )
    except Exception:
        import traceback
        traceback.print_exc()

    if not result:
        if( 0 < retryCount ):
            print( 'Retry')
            result, endFlag = RecvTestData( baseDir, socket, retryCount - 1 )
        else:
            raise ValueError( 'Failed to recv path' )

    return  ( result, endFlag )

# マスター登録処理
def ExecResist( sock, addr ):
    # プロセス ID 取得
    # LAN 内なので小さいデータでパケットロスは考えない
    newDir = ''
    try:
      pid=sock.recv( 1024 );
      newDir='{}/{}_{}'.format( TEMP_DIR, addr[0], pid.decode(ENCODE) )
      os.mkdir(newDir);
      while True:
          result, endFlag = RecvTestData( newDir, sock, DEFAULT_RETRY_COUNT )
          if endFlag:
              break;
    except OSError as e:
        print( 'Error : {0}'.format( e.strerror ) )
        if( 0 < len( newDir ) ):
            shutil.rmtree( newDir );
    except ValueError as e:
        print( 'Error : {0}'.format( e ) )
        if( 0 < len( newDir ) ):
            shutil.rmtree( newDir );
    except Exception:
        import traceback
        traceback.print_exc()
        if( 0 < len( newDir ) ):
            shutil.rmtree( newDir );

def MasterResist():
    try:
        resistSock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        resistSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        resistSock.bind((MASTER_IP, PORT_NUMBER))
        while True:
            resistSock.listen( 20 )
            newMasSock, newMasAdd = resistSock.accept()
            resistThread = \
                    threading.Thread( \
                         target=ExecResist, args=(newMasSock,newMasAdd,) )
            resistThread.start()
            break;

    except OSError as e:
        print( 'Error : {0}'.format( e.strerror ) )

if __name__ == "__main__":
    masterResist = threading.Thread( target=MasterResist )
    masterResist.start()
