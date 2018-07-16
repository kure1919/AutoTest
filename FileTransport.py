# coding : utf-8

import pickle
import os
import const

from logging import getLogger
logger = getLogger(__name__)

class FileTransporter:
    def SendDataUnit(self, socket, addr, path, virtPath, retryCount):
        result = False
        logger.debug( 'Send : ' + virtPath )
        isDir=os.path.isdir(path);
        try:
            # ファイル情報を送信
            data=pickle.dumps([const.TRANS_FILES, virtPath, isDir])
            logger.debug('Send file info to %s' % (addr[0]))
            socket.send(data)
            logger.debug('Recv send status from %s' % (addr[0]))
            status=socket.recv(1024)
            if const.SUCCESS_RECV != status:
                raise 'Fail send metadata'

            # ディレクトリでない場合はファイルデータを送信
            if not isDir:
                # 最初に転送するサイズを送信
                logger.debug('Send file data to %s' % (addr[0]))
                fileSize=os.path.getsize( path )
                socket.send(str( fileSize ).encode())
                if const.SUCCESS_RECV != socket.recv(1024):
                    raise ValueError('Fail send file size')

                f = open( path, 'rb' )
                sendData = f.read( 1024 )
                while 0 < len( sendData ):
                    socket.send( sendData )
                    sendData = f.read( 1024 )
                logger.debug('Recv file send status from %s' % (addr[0]))
                status=socket.recv(1024)
                if const.SUCCESS_RECV != status:
                    raise 'Fail send file'
            result = True
        except OSError as e:
            print( e.strerror )
        except ValueError as e:
            print( e )

        # リトライ
        if not result:
            if( 0 < retryCount ):
                print( 'Error detect retry')
                result = TransforTestData( socket, path, virtPath, retryCount - 1 )
            else:
                raise ValueError( 'Error SetUp' )

        return  result

    def RecurseSendData(self, socket, addr, realBase, virtBase):
        for path in os.listdir(realBase):
            realPath = path
            if 0 < len( realBase ):
                realPath = '%s/%s' % ( realBase, path )
            virtPath = path
            if 0 < len( virtBase ):
                virtPath = '%s/%s' % ( virtBase, path )
            self.SendDataUnit(socket, addr, realPath, virtPath, const.DEFAULT_RETRY_COUNT)
            # ディレクトリの場合再起的に転送
            if os.path.isdir( realPath ):
                self.RecurseSendData(socket, addr, realPath, virtPath)

    def SendEndInfo(self, socket, addr):
        logger.debug('Send end info to %s' % (addr[0]))
        data=pickle.dumps([const.END_TRANS, '', False])
        socket.send(data)
        logger.debug('Recv status info from %s' % (addr[0]))
        status=socket.recv(1024)
        if const.SUCCESS_RECV != status:
            logger.debug('Fail recv state from %s' % (addr[0]))
            logger.debug(status)
            raise ValueError( 'Fail send end info' )

    def RecvData(self, sock, addr, baseDir, retryCount):
        if ( baseDir == './' ) or ( 0 == len(baseDir) ):
            baseDir='.'

        if not os.path.exists(baseDir):
            os.makedirs(baseDir)

        result = False
        endFlag = False
        try:
            logger.debug('Recv command from %s' % (addr[0]))
            # ファイル情報を受信
            data = sock.recv(1024)
            # ( モード, パス, ディレクトリか否か ) の配列が飛んでくる
            data = pickle.loads(data)
            mode = data[0]
            path = data[1]
            isDir = data[2]
            logger.debug('Recv(from %s): %s' % (addr[0], path))

            # 成功ステータスを返却
            if const.TRANS_FILES == mode:
                logger.debug('Send recv status to %s' % (addr[0]))
                sock.send(const.SUCCESS_RECV)
                fullPath = '{}/{}'.format(baseDir, path)
                if isDir:
                    logger.debug('Make dir(%s)' % (fullPath))
                    # ディレクトリの場合はディレクトリを作成
                    os.makedirs(fullPath)
                else:
                    fileSize=int(sock.recv(1024).decode(const.ENCODE))
                    logger.debug('Recv File size(from %s): %s' % (addr[0], str( fileSize )))
                    sock.send(const.SUCCESS_RECV)

                    # ディレクトリでない場合はファイルデータを受信
                    f = open(fullPath, 'wb')
                    tmpSize=0
                    while 0 != fileSize:
                        tmp = sock.recv(2048)
                        if 0 < len(tmp):
                            f.write(tmp)
                        tmpSize += len(tmp)
                        if fileSize <= tmpSize:
                            break;
                    logger.debug('Send recv status to %s' % (addr[0]))
                    sock.send(const.SUCCESS_RECV)
                result = True
            elif const.END_TRANS == mode:
                logger.debug('Send recv status to %s' % (addr[0]))
                sock.send(const.SUCCESS_RECV)
                logger.debug('End trans from %s' % (addr[0]))
                endFlag = True
                result = True
            else:
                logger.debug('Send recv status to %s' % (addr[0]))
                sock.send(const.FAIL_RECV)
                raise ValueError('mode is invalid.')
        except OSError as e:
            logger.debug('Send recv status to %s' % (addr[0]))
            sock.send(const.FAIL_RECV)
            print(e.strerror)
        except Exception:
            import traceback
            traceback.print_exc()
            logger.debug('Send recv status to %s' % (addr[0]))
            sock.send(pickle.dumps([const.FAIL_RECV, baseDir]))

        if not result:
            if 0 < retryCount:
                print('Error detect retry')
                result, endFlag = self.RecvData(sock, addr, baseDir, retryCount - 1)
            else:
                raise ValueError('Failed to recv path')

        return (result, endFlag)

