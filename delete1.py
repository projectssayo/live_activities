import os
import base64
import hashlib
import math
import threading
import sqlite3
import time
import traceback
import re
from datetime import datetime,timezone,timedelta
import socket

from PyQt5.QtCore import QByteArray,QUrl,QSize,QMutex,QMutexLocker,QThreadPool,QEvent,QCoreApplication
from PyQt5.QtGui import QPixmap, QPainter, QStandardItemModel, QStandardItem, QPixmapCache, QMovie, QImage, QTextOption, \
    QTextCursor
from PyQt5.QtNetwork import QNetworkAccessManager,QNetworkRequest
from PyQt5.QtSvg import QSvgRenderer
from PyQt5.QtWidgets import (QGraphicsDropShadowEffect,QApplication,QMainWindow,QStackedWidget,QLabel,QPushButton,QVBoxLayout,QWidget,QHBoxLayout,QFrame , QLineEdit,QTextEdit,QTextBrowser,QButtonGroup,QAbstractItemView)
from PyQt5.QtGui import QColor, QFont, QFontMetrics, QPixmap, QPainter, QBrush, QPen, QLinearGradient, QPainterPath, \
    QIcon
from PyQt5.QtCore import QThread,pyqtSignal,Qt,QPropertyAnimation,QEasingCurve ,QPoint,QRect,QTimer,QRectF
from PyQt5.uic import loadUi
import sys

from cryptography.fernet import Fernet

from utils_.temp_test_info import return_data
from typing import Union,Optional,Tuple, List

from utils_.all_theme_functions_old import setup_theme_db,add_user_theme,get_user_theme,setup_blade_runner_theme
import secrets
import string
from PyQt5 import sip
setup_theme_db()
setup_blade_runner_theme()
add_user_theme('suyognegi1@gmail.com',1)

current_mac_id='xyz'
CHAT_PAGE_SIZE=20
import time
import json

import requests
import websocket
from PyQt5.QtCore import QThread,pyqtSignal
import time
import requests
from PyQt5.QtCore import QThread,pyqtSignal

import socket
import time
from PyQt5.QtCore import QThread,pyqtSignal
from PyQt5.QtCore import Qt


class InternetMonitorThread(QThread):
    went_offline = pyqtSignal()
    came_online = pyqtSignal(float)

    def __init__(self, check_interval=3):
        super().__init__()
        self.check_interval = check_interval
        self._running = True
        self._is_online = None
        self._offline_since = None

    def is_online(self):
        try:
            s = socket.create_connection(("1.1.1.1", 53), timeout=3)
            s.close()
            return True
        except (OSError, socket.timeout):
            return False

    def run(self):
        while self._running and not self.isInterruptionRequested():
            try:
                online = self.is_online()

                if self._is_online is None:
                    # first check after startup
                    self._is_online = online
                    if not online:
                        # started offline: remember when, so came_online works later
                        self._offline_since = time.time()
                        self.went_offline.emit()

                elif self._is_online and not online:
                    self._is_online = False
                    self._offline_since = time.time()
                    self.went_offline.emit()

                elif not self._is_online and online:
                    self._is_online = True
                    since = self._offline_since if self._offline_since is not None else time.time()
                    offline_duration = time.time() - since
                    self._offline_since = None
                    self.came_online.emit(offline_duration)
            except Exception as e:
                # never let the monitor thread die
                print(f'[internet monitor] error: {e}')

            self.msleep(int(self.check_interval * 1000))

    def stop(self):
        self._running = False
        self.requestInterruption()
        self.wait()



class LiveActivityWebSocketThread(QThread):
    connected=pyqtSignal()
    disconnected= pyqtSignal()
    presence_update=pyqtSignal(dict)
    error=pyqtSignal(str)
    sync_ack=pyqtSignal(list)
    new_message_received=pyqtSignal(dict)
    message_edited=pyqtSignal(str,str)
    message_deleted_all=pyqtSignal(str)

    BASE_LIVE_ACTIVITY="https://live-activities-4jmu.onrender.com"

    def __init__(self,email,mac_id,parent=None):
        super().__init__(parent)
        self.email=email
        self.mac_id=mac_id
        self._should_run=True
        self.ws=None

    def _ws_url(self):
        base=self.BASE_LIVE_ACTIVITY.replace("https://","wss://").replace("http://","ws://")
        return f"{base}/ws/{self.email}/{self.mac_id}"

    def run(self):
        attempt=0
        while self._should_run:
            attempt+=1
            print(f"[ws] connecting (attempt {attempt}) as {self.email}")
            try:
                self.ws=websocket.WebSocketApp(self._ws_url(),on_open=self._on_open,on_message=self._on_message ,on_close=self._on_close,on_error=self._on_error,)

                self.ws.run_forever(ping_interval=25,ping_timeout=10)
            except Exception as e:
                print(f"[ws] run_forever raised: {e}")

            if not self._should_run:
                break
            print("[ws] disconnected, retrying in 3s")
            time.sleep(3)

    def _on_open(self,ws):
        print("[ws] CONNECTED")
        self.connected.emit()

    def _on_message(self,ws,message):
        try:
            data=json.loads(message)
        except (TypeError,ValueError):
            print(f"[ws] non-json message ignored: {message!r}")
            return

        msg_type=data.get("type")
        if msg_type!="ping":
            print(f"[ws] recv type={msg_type}")

        if msg_type=="ping":
            self._send({"type":"pong"})
            return

        if msg_type=="presence_update":
            self.presence_update.emit(data)
            return

        if msg_type=="bulk_presence":
            for update in data.get("updates",[]):
                self.presence_update.emit(update)
            return

        if msg_type=="sync_last_clicked_ack":
            self.sync_ack.emit(data.get("chat_ids")or[])
            return

        if msg_type=="new_message":
            self.new_message_received.emit(data.get("message")or{})
            return

        if msg_type=="message_edited":
            self.message_edited.emit(data.get("_id"),data.get("msg_content"))
            return

        if msg_type=="message_deleted_all":
            self.message_deleted_all.emit(data.get("_id"))
            return

        print(f"[ws] UNHANDLED message type: {msg_type} -> {data}")

    def _on_close(self,ws,close_status_code,close_msg):
        print(f"[ws] CLOSED code={close_status_code} msg={close_msg}")
        self.disconnected.emit()

    def _on_error(self,ws ,error):
        print(f"[ws] ERROR {error}")
        self.error.emit(str(error))

    def open_chat(self,target_email):
        self._send({"type" :"opened_chat","target_email":target_email})

    def close_chat(self):
        self._send({"type":"closed_chat"})

    def request_sync(self,friend_emails):
        self._send({"type":"sync_request","friend_list":list(friend_emails)})

    def push_last_clicked_batch(self,entries):
        if entries:
            self._send({"type":"sync_last_clicked","entries":entries})

    def is_connected(self):
        return self.ws is not None and self.ws.sock and self.ws.sock.connected

    def _send(self,payload):
        if self.ws is not None and self.ws.sock and self.ws.sock.connected:
            def _do_send():
                try:
                    self.ws.send(json.dumps(payload))
                except Exception as e:
                    self.error.emit(str(e))

            threading.Thread(target=_do_send,daemon=True).start()
        else:
            print(f"[ws] NOT connected, dropped outgoing {payload.get('type')}")

    def stop(self):
        self._should_run=False
        if self.ws is not None:
            self.ws.close()









class FriendStatusPollThread(QThread):
    status_received=pyqtSignal(dict)
    error= pyqtSignal(str)

    BASE_LIVE_ACTIVITY="https://live-activities-4jmu.onrender.com"

    def __init__(self,my_email,friend_email,interval_seconds=0,parent=None):

        super().__init__(parent)
        self.my_email=my_email
        self.friend_email=friend_email
        self.interval_seconds =interval_seconds
        self._should_run= True

    def run(self):
        while self._should_run:
            try:
                resp=requests.get(f"{self.BASE_LIVE_ACTIVITY}/status",params={"me" :self.my_email,"friend":self.friend_email},timeout=8,)
                resp.raise_for_status()
                self.status_received.emit(resp.json())
            except Exception as e:
                self.error.emit(str(e))

            if self.interval_seconds<=0:
                break
            for _ in range(self.interval_seconds*10):
                if not self._should_run:
                    break

                time.sleep(0.1)

    def stop(self):
        self._should_run=False


class MacWatcherThread(QThread):
    mac_changed=pyqtSignal(str)
    error=pyqtSignal(str)

    BASE_LIVE_ACTIVITY="https://live-activities-4jmu.onrender.com"

    def __init__(self,email,current_mac_id,api_secret=None,poll_seconds =15,parent=None):
        super().__init__(parent)
        self.email=email
        self.current_mac_id = current_mac_id
        self.api_secret=api_secret
        self.poll_seconds=poll_seconds
        self._should_run=True

    def run(self):
        while self._should_run:
            try:
                headers={"x-api-secret":self.api_secret} if self.api_secret else {}
                resp=requests.get(f"{self.BASE_LIVE_ACTIVITY}/check_mac/{self.email}",params={"mac_id":self.current_mac_id},headers=headers,timeout=8,)
                resp.raise_for_status()
                data=resp.json()
                if not data.get("match",True):
                    self.mac_changed.emit(data.get("registered_mac"))
            except Exception as e:
                self.error.emit(str(e))

            for _ in range(self.poll_seconds*10):
                if not self._should_run:
                    break

                time.sleep(0.1)


    def stop(self):
        self._should_run=False


class start_expanding(QThread):
    signal=pyqtSignal(bool,int,int)
    finished=pyqtSignal(bool)

    def __init__(self):
        super().__init__()
        self.width=861

    def run(self):
        temp=0
        for i in range(180,40,-20):
            self.signal.emit(True,i,self.width+20*temp)
            temp+=1
            QThread.msleep(1)


class start_contracting(QThread):
    signal=pyqtSignal(bool,int,int)
    finished=pyqtSignal(bool)

    def __init__(self):
        super().__init__()
        self.width=961

    def run(self):
        temp=5
        for i in range(60,200,20):
            self.signal.emit(True , i,self.width-20*temp)


class LastSeenLocalDB:

    def __init__(self,db_path:str):
        self.db_path=db_path
        self._lock=threading.Lock()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn=sqlite3.connect(self.db_path,timeout=10,check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _init_db(self) -> None:
        conn=self._get_conn()
        try:
            conn.execute("""CREATE TABLE IF NOT EXISTS last_seen ( _id TEXT PRIMARY KEY, last_seen_at TEXT, is_online INTEGER )""")
            conn.commit()
        finally:
            conn.close()

    def read_all(self) -> dict:
        with self._lock:
            conn=self._get_conn()
            try:
                cur=conn.execute("SELECT _id, last_seen_at, is_online FROM last_seen")
                rows=cur.fetchall()
            finally:
                conn.close()
        return {row[0]:{"is_online" :bool(row[2]),"last_seen_at":row[1]}for row in rows}

    def read_one(self,email:str) -> dict | None:
        with self._lock:
            conn=self._get_conn()
            try:
                cur=conn.execute("SELECT _id, last_seen_at, is_online FROM last_seen WHERE _id = ?",(email,),)
                row=cur.fetchone()
            finally:
                conn.close()
        if row is None:
            return None
        return {"is_online":bool(row[2]),"last_seen_at":row[1]}

    def upsert(self ,email:str,last_seen_at,is_online:bool) -> None:
        if isinstance(last_seen_at,datetime):
            last_seen_at=last_seen_at.isoformat()
        with self._lock:
            conn=self._get_conn()
            try:
                conn.execute("""INSERT INTO last_seen (_id,last_seen_at,is_online) VALUES (?, ?, ?) ON CONFLICT(_id) DO UPDATE SET last_seen_at=excluded.last_seen_at, is_online=excluded.is_online""",(email,last_seen_at,int(bool(is_online))),)
                conn.commit()
            except sqlite3.OperationalError as e:
                print(f"error in upsert: {e}")
            finally:
                conn.close()

    def upsert_many(self ,entries:dict) -> None:
        rows=[(email,data.get("last_seen_at"),int(bool(data.get("is_online"))))for email,data in entries.items()]
        if not rows:
            return
        with self._lock:
            conn=self._get_conn()
            try:
                conn.executemany("""INSERT INTO last_seen (_id,last_seen_at,is_online) VALUES (?, ?, ?) ON CONFLICT(_id) DO UPDATE SET last_seen_at=excluded.last_seen_at, is_online=excluded.is_online""",rows,)
                conn.commit()
            except sqlite3.OperationalError as e:
                print(f"error in upsert_many: {e}")
            finally:
                conn.close()

    def delete_missing(self,keep_emails:list) -> None:
        if not keep_emails:
            return
        with self._lock:
            conn=self._get_conn()
            try:
                placeholders=",".join("?" for _ in keep_emails)
                conn.execute(f"DELETE FROM last_seen WHERE _id NOT IN ({placeholders})",keep_emails ,)
                conn.commit()
            finally:
                conn.close()




class LastSeenSyncThread(QThread):
    friend_status_changed=pyqtSignal(str,bool,str)
    sync_complete=pyqtSignal(dict)
    error=pyqtSignal(str)

    def __init__(self,current_user_email:str,friend_list:list,local_db:LastSeenLocalDB,api_base_url:str,local_cache_snapshot:dict,parent= None,):
        super().__init__(parent)
        self.current_user_email=current_user_email
        self.friend_list = list(friend_list)
        self.local_db=local_db
        self.api_base_url=api_base_url
        self.local_cache_snapshot=local_cache_snapshot or {}

    def run(self):
        if not self.friend_list:
            self.sync_complete.emit({})
            return

        try:
            resp= requests.get(f"{self.api_base_url}/friends_last_seen",params={"emails":",".join(self.friend_list)},timeout=10,)
            resp.raise_for_status()
            remote_data=resp.json()
        except Exception as e:
            self.error.emit(f"friends_last_seen fetch failed: {e}")
            return

        changed={}
        for i in self.friend_list:
            remote=remote_data.get(i)
            if remote is None:
                continue

            local=self.local_cache_snapshot.get(i)
            if (local is None or local.get("is_online")!=remote.get("is_online")or local.get("last_seen_at")!=remote.get("last_seen_at")):
                changed[i]=remote

        if changed:
            try:
                self.local_db.upsert_many(changed)
            except Exception as e:
                self.error.emit(f"local sqlite upsert failed: {e}")
            for i,data in changed.items():
                self.friend_status_changed.emit(i,bool(data.get("is_online")) ,data.get("last_seen_at")or "")

        self.sync_complete.emit(remote_data)


def sanitize_email(email:str) -> str:
    return email.replace(".","dot").replace("@","at")


def get_chat_id(email1:str,email2:str) -> str:
    if email1 < email2:
        return f"chat_{email2}_{email1}"
    return f"chat_{email1}_{email2}"

def get_local_chat_id(email_a:str,email_b:str) -> str:
    return f"chat_{min(email_a, email_b)}_{max(email_a, email_b)}"
    
def msg_time(m):
    """Effective time of a message: received_at if present, else sent_at."""
    return m.get('received_at') or m.get('sent_at')

class FetchOlderMessagesThread(QThread):
    finished_ok=pyqtSignal(list)
    error=pyqtSignal(str)
    BASE_LIVE_ACTIVITY = "https://live-activities-4jmu.onrender.com"

    def __init__(self,user_a,user_b,before_sent_at=None,limit=CHAT_PAGE_SIZE):
        super().__init__()
        self.user_a=user_a
        self.user_b=user_b
        self.before_sent_at= before_sent_at
        self.limit=limit

    def run(self):
        try:
            params={"user_a":self.user_a,"user_b":self.user_b,"limit":self.limit}
            if self.before_sent_at:
                params["before_sent_at"] = self.before_sent_at
            resp=requests.get(f"{self.BASE_LIVE_ACTIVITY}/get_messages_page",params=params,timeout= 10)
            resp.raise_for_status()
            data= resp.json()
            self.finished_ok.emit(data.get("messages",[]))
        except Exception as e:
            self.error.emit(str(e))


class SendMessageThread(QThread):
    finished_ok = pyqtSignal(dict)
    error = pyqtSignal(str)
    BASE_LIVE_ACTIVITY = "https://live-activities-4jmu.onrender.com"

    def __init__(self, payload):
        super().__init__()
        self.payload = payload

    def run(self):
        try:
            resp = requests.post(f"{self.BASE_LIVE_ACTIVITY}/send_message", json=self.payload, timeout=10)
            if resp.status_code >= 400:
                self.error.emit(f"HTTP {resp.status_code}: {resp.text[:300]}")
                return
            try:
                data = resp.json()
            except Exception:
                data = {}
            self.finished_ok.emit(data if isinstance(data, dict) else {})
        except Exception as e:
            self.error.emit(str(e))


            


class ImageUploadThread(QThread):
    finished_ok=pyqtSignal(str,str)
    error= pyqtSignal(str)
    BASE_LIVE_ACTIVITY="https://live-activities-4jmu.onrender.com"

    def __init__(self,file_path,msg_id):
        super().__init__()
        self.file_path=file_path
        self.msg_id=msg_id

    def run(self):
        try:
            with open(self.file_path,'rb') as f:
                files={'file':f}
                data={'msg_id':self.msg_id}
                resp=requests.post(f"{self.BASE_LIVE_ACTIVITY}/upload_chat_image",files=files,data=data,timeout=30)
            resp.raise_for_status()
            result=resp.json()
            if result.get('ok'):
                self.finished_ok.emit(result['url'],result['thumbnail_url'])
            else:
                self.error.emit(result.get('error','unknown upload error'))
        except Exception as e:
            self.error.emit(str(e))


class ImageDownloadThread(QThread):
    finished_ok=pyqtSignal(str,str)
    finished_error=pyqtSignal(str)

    def __init__(self,thumbnail_url,full_url,msg_id):
        super().__init__()
        self.thumbnail_url=thumbnail_url
        self.full_url=full_url
        self.msg_id=msg_id

    def run(self):
        import urllib.request
        try:
            os.makedirs("thumbnail_images",exist_ok=True)
            os.makedirs("user_images",exist_ok=True)
            thumb_path=os.path.join("thumbnail_images",f"{self.msg_id}_thumbnail.jpg")
            full_path=os.path.join("user_images",f"{self.msg_id}.jpg")

            req_thumb=urllib.request.Request(self.thumbnail_url,headers={"User-Agent":"Mozilla/5.0"})
            with urllib.request.urlopen(req_thumb,timeout=10) as r:
                open(thumb_path,"wb").write(r.read())

            req_full=urllib.request.Request(self.full_url,headers={"User-Agent":"Mozilla/5.0"})
            with urllib.request.urlopen(req_full,timeout=10) as r:
                open(full_path , "wb").write(r.read())

            self.finished_ok.emit(thumb_path,full_path)
        except Exception as e:
            self.finished_error.emit(str(e))



class LastClickedLocalDB:

    def __init__(self,db_path:str="last_clicked.db"):
        self.db_path = db_path
        self._lock=threading.Lock()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn=sqlite3.connect(self.db_path,timeout=10,check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _init_db(self) -> None:
        conn=self._get_conn()
        try:
            conn.execute("""CREATE TABLE IF NOT EXISTS chat_last_clicked ( chat_id TEXT PRIMARY KEY, data TEXT, synced INTEGER DEFAULT 0 )""")
            try:
                conn.execute("ALTER TABLE chat_last_clicked ADD COLUMN synced INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            conn.commit()
        finally:
            conn.close()

    def read_all(self) -> dict:
        with self._lock:
            conn=self._get_conn()
            try:
                cur=conn.execute("SELECT chat_id, data FROM chat_last_clicked")
                rows=cur.fetchall()
            finally:
                conn.close()
        result={}
        for chat_id,data in rows:
            try:
                result[chat_id]=json.loads(data)
            except Exception:
                result[chat_id]={}



        return result

    def read_unsynced(self) -> list:
        with self._lock:
            conn=self._get_conn()
            try:
                cur=conn.execute("SELECT chat_id, data FROM chat_last_clicked WHERE synced = 0")
                rows=cur.fetchall()
            finally:
                conn.close()
        result=[]
        for chat_id,data in rows:
            try:
                result.append((chat_id,json.loads(data)))
            except Exception:
                pass

        return result

    def upsert_field(self,chat_id:str,field:str,value_iso_or_none) -> dict:
        with self._lock:
            conn= self._get_conn()
            try:
                cur=conn.execute("SELECT data FROM chat_last_clicked WHERE chat_id = ?",(chat_id,))
                row=cur.fetchone()
                existing=json.loads(row[0]) if row else {}
                existing[field]=value_iso_or_none
                serialized=json.dumps(existing ,sort_keys=True)
                conn.execute("""INSERT INTO chat_last_clicked (chat_id,data,synced) VALUES (?, ?,0) ON CONFLICT(chat_id) DO UPDATE SET data=excluded.data,synced=0""" , (chat_id,serialized),)
                conn.commit()
            except sqlite3.OperationalError as e:
                print(f"error in upsert_field: {e}")
                existing={}
            finally:
                conn.close()
        return existing

    def mark_synced(self,chat_id:str,data_at_push_time:dict) -> None:
        target=json.dumps(data_at_push_time,sort_keys=True)
        with self._lock:
            conn=self._get_conn()
            try:
                cur=conn.execute("SELECT data FROM chat_last_clicked WHERE chat_id = ?",(chat_id,))
                row=cur.fetchone()
                if row and row[0]==target:
                    conn.execute("UPDATE chat_last_clicked SET synced = 1 WHERE chat_id = ?",(chat_id,))
                    conn.commit()
            except sqlite3.OperationalError as e:
                print(f"error in mark_synced: {e}")
            finally:
                conn.close()



class LastClickedSafetyNetThread(threading.Thread):
    def __init__(self,flush_callback,interval_seconds:int=20):
        super().__init__(daemon=True)
        self.flush_callback=flush_callback
        self.interval_seconds=interval_seconds
        self._should_run=True

    def run(self):
        while self._should_run:
            time.sleep(self.interval_seconds)
            if not self._should_run:
                break
            try:
                self.flush_callback()
            except Exception as e:
                print(f"[last_clicked safety-net] error: {e}")

    def stop(self):
        self._should_run=False


from PyQt5.QtCore import QThread,pyqtSignal

from PyQt5.QtCore import QThread,pyqtSignal


class ImageSaveThread(QThread):
    finished_ok=pyqtSignal(str,str,str,int,int)
    finished_error=pyqtSignal(str)

    def __init__(self,save_func, file_path,id1,quality=20):
        super().__init__()
        self.save_func=save_func
        self.file_path=file_path
        self.id1= id1
        self.quality= quality

    def run(self):
        try:
            thumbnail_path,user_image_path,width,height=self.save_func(self.file_path,self.id1,self.quality)
            if thumbnail_path is None:
                self.finished_error.emit("Could not process the image.")
            else:
                self.finished_ok.emit(thumbnail_path,user_image_path,self.id1,width or 0,height or 0)
        except Exception as e:
            self.finished_error.emit(str(e))




class MessagesLocalDB:
    def __init__(self, db_path="messages.db"):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _init_db(self):
        conn = self._get_conn()
        try:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS messages ( _id TEXT PRIMARY KEY, msg_type TEXT, msg_content TEXT, thumbnail_url TEXT, local_thumbnail_path TEXT, local_image_path TEXT, is_edited INTEGER DEFAULT 0, sent_by TEXT, sent_to TEXT, sent_at TEXT, delete_from_me INTEGER DEFAULT 0, delete_from_all INTEGER DEFAULT 0, chat_id TEXT, width INTEGER, height INTEGER, synced INTEGER DEFAULT 1, received_at TEXT )""")
            for ddl in ("ALTER TABLE messages ADD COLUMN width INTEGER",
                        "ALTER TABLE messages ADD COLUMN height INTEGER",
                        "ALTER TABLE messages ADD COLUMN synced INTEGER DEFAULT 1",
                        "ALTER TABLE messages ADD COLUMN received_at TEXT"):
                try:
                    conn.execute(ddl)
                except sqlite3.OperationalError:
                    pass
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_sent_at ON messages(chat_id, sent_at)")
            conn.commit()
        finally:
            conn.close()

    def mark_deleted_for_all(self, msg_id):
        with self._lock:
            conn = self._get_conn()
            try:
                cur = conn.execute(
                    "UPDATE messages SET delete_from_all=1, msg_type='delete_from_everyone' WHERE _id=?", (msg_id,))
                conn.commit()
                if cur.rowcount == 0:
                    return None
                cols = [c[1] for c in conn.execute("PRAGMA table_info(messages)").fetchall()]
                row = conn.execute("SELECT * FROM messages WHERE _id=?", (msg_id,)).fetchone()
            except sqlite3.OperationalError as e:
                print(f"error in mark_deleted_for_all: {e}")
                return None
            finally:
                conn.close()
        return dict(zip(cols, row)) if row else None

    def _set_synced(self, msg_id, value):
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("UPDATE messages SET synced=? WHERE _id=?", (value, msg_id))
                conn.commit()
            except sqlite3.OperationalError as e:
                print(f"error in _set_synced: {e}")
            finally:
                conn.close()

    def mark_synced(self, msg_id):
        self._set_synced(msg_id, 1)

    def mark_unsynced(self, msg_id):
        self._set_synced(msg_id, 0)

    def set_received_at(self, msg_id, received_at):
        if not received_at:
            return
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("UPDATE messages SET received_at=? WHERE _id=?", (received_at, msg_id))
                conn.commit()
            except sqlite3.OperationalError as e:
                print(f"error in set_received_at: {e}")
            finally:
                conn.close()

    def get_unsynced(self, sender_email):
        with self._lock:
            conn = self._get_conn()
            try:
                cols = [c[1] for c in conn.execute("PRAGMA table_info(messages)").fetchall()]
                rows = conn.execute("SELECT * FROM messages WHERE synced=0 AND sent_by=? ORDER BY sent_at ASC",
                                    (sender_email,)).fetchall()
            finally:
                conn.close()
        return [dict(zip(cols, r)) for r in rows]

    def upsert_message(self, msg: dict, chat_id: str):
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    """INSERT INTO messages (_id, msg_type, msg_content, thumbnail_url, local_thumbnail_path, local_image_path, is_edited, sent_by, sent_to, sent_at, delete_from_me, delete_from_all, chat_id, width, height, received_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(_id) DO UPDATE SET msg_content=excluded.msg_content, thumbnail_url=excluded.thumbnail_url, local_thumbnail_path=COALESCE(excluded.local_thumbnail_path, local_thumbnail_path), local_image_path=COALESCE(excluded.local_image_path, local_image_path), is_edited=excluded.is_edited, delete_from_me=excluded.delete_from_me, delete_from_all=excluded.delete_from_all, width=COALESCE(excluded.width, width), height=COALESCE(excluded.height, height), received_at=COALESCE(excluded.received_at, received_at)""",
                    (msg["_id"], msg["msg_type"], msg["msg_content"], msg.get("thumbnail_url"),
                     msg.get("local_thumbnail_path"), msg.get("local_image_path"),
                     int(msg.get("is_edited", False)), msg["sent_by"], msg["sent_to"], msg["sent_at"],
                     int(msg.get("delete_from_me", False)), int(msg.get("delete_from_all", False)),
                     chat_id, msg.get("width"), msg.get("height"), msg.get("received_at")))
                conn.commit()
            except sqlite3.OperationalError as e:
                print(f"error in upsert_message: {e}")
            finally:
                conn.close()

    def get_latest(self, chat_id, limit=CHAT_PAGE_SIZE):
        with self._lock:
            conn = self._get_conn()
            try:
                cols = [c[1] for c in conn.execute("PRAGMA table_info(messages)").fetchall()]
                rows = conn.execute(
                    "SELECT * FROM messages WHERE chat_id=? ORDER BY COALESCE(received_at, sent_at) DESC LIMIT ?",
                    (chat_id, limit)).fetchall()
            finally:
                conn.close()
        return [dict(zip(cols, r)) for r in rows][::-1]

    def get_by_id(self, msg_id):
        with self._lock:
            conn = self._get_conn()
            try:
                cols = [c[1] for c in conn.execute("PRAGMA table_info(messages)").fetchall()]
                row = conn.execute("SELECT * FROM messages WHERE _id=?", (msg_id,)).fetchone()
            finally:
                conn.close()
        if row is None:
            return None
        return dict(zip(cols, row))

    def get_before(self, chat_id, before_sent_at, limit=CHAT_PAGE_SIZE):
        with self._lock:
            conn = self._get_conn()
            try:
                cols = [c[1] for c in conn.execute("PRAGMA table_info(messages)").fetchall()]
                rows = conn.execute(
                    "SELECT * FROM messages WHERE chat_id=? AND COALESCE(received_at, sent_at) < ? "
                    "ORDER BY COALESCE(received_at, sent_at) DESC LIMIT ?",
                    (chat_id, before_sent_at, limit)).fetchall()
            finally:
                conn.close()
        return [dict(zip(cols, r)) for r in rows][::-1]

    def clear_all(self):
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("DELETE FROM messages")
                conn.commit()
            finally:
                conn.close()




class DeleteForAllThread(QThread):
    finished_ok=pyqtSignal()
    error=pyqtSignal(str)
    BASE_LIVE_ACTIVITY="https://live-activities-4jmu.onrender.com"

    def __init__(self,msg_id,notify_email):
        super().__init__()
        self.msg_id=msg_id
        self.notify_email=notify_email

    def run(self):
        try:
            resp=requests.post(f"{self.BASE_LIVE_ACTIVITY}/mark_delete_for_all/{self.msg_id}",json ={"notify_email":self.notify_email},timeout=10)
            resp.raise_for_status()
            self.finished_ok.emit()
        except Exception as e:
            self.error.emit(str(e))
class DeleteForMeThread(QThread):
    finished_ok=pyqtSignal()
    error= pyqtSignal(str)
    BASE_LIVE_ACTIVITY="https://live-activities-4jmu.onrender.com"

    def __init__(self,msg_id,who):
        super().__init__()
        self.msg_id=msg_id
        self.who=who

    def run(self):
        try:
            resp = requests.post(f"{self.BASE_LIVE_ACTIVITY}/mark_delete_for_me/{self.msg_id}",json={"who":self.who},timeout=10)
            resp.raise_for_status()
            self.finished_ok.emit()
        except Exception as e:
            self.error.emit(str(e))
from profile_pic_and_basic_user_info import basic_data


from PyQt5.QtCore import QAbstractAnimation


class LoadingSpinner(QWidget):
    def __init__(self,parent=None,size=34):
        super().__init__(parent)
        self.setFixedSize(size,size)
        self.setAttribute(Qt.WA_TransparentForMouseEvents)
        self._angle=0
        self._timer=QTimer(self)
        self._timer.timeout.connect(self._tick)
        self.hide()

    def start(self):
        self._timer.start(50)
        self.show()
        self.raise_()

    def stop(self):
        self._timer.stop()
        self.hide()

    def _tick(self):
        self._angle=(self._angle+30) % 360
        self.update()

    def paintEvent(self,_):
        p=QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        p.setPen(Qt.NoPen)
        p.setBrush(QColor(0,0,0,110))
        p.drawEllipse(self.rect())
        p.translate(self.width()/2,self.height()/2)
        p.rotate(self._angle)
        for i in range(12):
            p.setBrush(QColor(255,255,255,int(255*(1-i/12))))
            p.drawRoundedRect(QRectF(-1.5,-self.height()/2+6,3,7),1.5 ,1.5)
            p.rotate(-30)
class LastMessagePreviewLocalDB:

    def __init__(self,current_user_email:str,db_path:str ="last_message_preview.db"):
        self.table="t_" + re.sub(r'[^a-zA-Z0-9_]','_',current_user_email)
        self.db_path=db_path
        self._lock=threading.Lock()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn=sqlite3.connect(self.db_path,timeout=10 ,check_same_thread = False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _init_db(self) -> None:
        conn=self._get_conn()
        try:
            conn.execute(f"""CREATE TABLE IF NOT EXISTS {self.table} ( friend_email TEXT PRIMARY KEY, data TEXT )""")
            conn.commit()
        finally:
            conn.close()

    def read_all(self) -> dict:
        with self._lock:
            conn=self._get_conn()
            try:
                rows= conn.execute(f"SELECT friend_email, data FROM {self.table}").fetchall()
            finally:
                conn.close()
        out={}
        for friend_email,data in rows:
            try:
                out[friend_email]=json.loads(data) if data else None
            except Exception:
                out[friend_email]=None
        return out

    def upsert_many(self,entries:dict) -> None:
        rows=[(friend_email,json.dumps(entry)if entry else None)for friend_email,entry in entries.items()]
        if not rows:
            return
        with self._lock:
            conn=self._get_conn()
            try:
                conn.executemany(f"""INSERT INTO {self.table} (friend_email, data) VALUES (?, ?) ON CONFLICT(friend_email) DO UPDATE SET data=excluded.data""",rows,)
                conn.commit()
            except sqlite3.OperationalError as e:
                print(f"error in LastMessagePreviewLocalDB.upsert_many: {e}")
            finally:
                conn.close()


class FetchLastMessagesPreviewThread(QThread):
    finished_ok=pyqtSignal(dict)
    error=pyqtSignal(str)
    BASE_LIVE_ACTIVITY="https://live-activities-4jmu.onrender.com"

    def __init__(self,user_email):
        super().__init__()
        self.user_email=user_email

    def run(self):
        try:
            resp=requests.get(f"{self.BASE_LIVE_ACTIVITY}/last_messages_preview",params={"user_email" : self.user_email} ,timeout=10,)
            resp.raise_for_status()
            data=resp.json()
            if data.get("ok"):
                self.finished_ok.emit(data.get("friend_list_last_messages",{}))
            else:
                self.error.emit(data.get("error","unknown error"))
        except Exception as e:
            self.error.emit(str(e))
class FriendEntry(dict):

    def __init__(self,*args,on_time_changed=None,**kwargs):
        super().__init__(*args,**kwargs)
        self._on_time_changed=on_time_changed

    def __setitem__(self,key,value):
        old=self.get(key)
        super().__setitem__(key,value)
        if key=='last_msg_sent_at' and old!=value and self._on_time_changed:
            self._on_time_changed()


class chating_main(QMainWindow):
    def __init__(self):
        super().__init__()
        loadUi("friend_list3.ui",self)
        print(datetime.now())
        user_info= basic_data.user_data_chandler
        print(f'user_info = {user_info}')
        self.current_user_email=user_info['current_user']['email']
        self.current_user_data=user_info['current_user']
        self.all_user_basic_info=user_info['all_users']

        self.friend_list = user_info['friend_list']
        self.incoming_list=user_info['incoming_list']
        self.outgoing_list=user_info['outgoing_list']
        self.blocked_list= user_info['blocked_list']
        self.set_rounded_corners(self.a_label)
        self.set_svg_icon_and_color(self.gif_button , 'svg_icons/gif.svg','#fefefe')
        self.current_central_chat_frame=None

        self.friend_list_left_label_dict={}
        self.last_message_preview_db=LastMessagePreviewLocalDB(self.current_user_email)

        def _kick_off_preview_refresh():

            cached=self.last_message_preview_db.read_all()
            for friend_email,entry in cached.items():
                self.apply_last_message_preview(friend_email,entry)


            t=FetchLastMessagesPreviewThread(self.current_user_email)
            t.finished_ok.connect(self.on_last_messages_preview_fetched)
            t.error.connect(lambda e:print(f'[last_messages_preview] fetch failed: {e}'))
            self._track_thread(t)

        QTimer.singleShot(2100 ,_kick_off_preview_refresh)
        self._init_chat_scroll()

        self._click_threads=[]

        from utils_.theme_direct import theme_one

        self.current_chat_theme_styleSheet=theme_one()

        print(f'self.current_chat_theme_styleSheet = {self.current_chat_theme_styleSheet}')

        self.friend_list_model=QStandardItemModel()
        self.friend_list_view.setModel(self.friend_list_model)
        self.friend_list_view.setViewportMargins(8,0,0,0)
        self.friend_list_view.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.friend_list_view.verticalScrollBar().setSingleStep(15)

        QTimer.singleShot(2000,lambda:self.start_loading_friend_list_label())
        QTimer.singleShot(2000,lambda:(self.set_shadow_label_or_frame(self.friend_list_frame,shadow='mid',color_scheme='light'), self.set_shadow_label_or_frame(self.friend_right_frame,shadow='mid' , color_scheme='light'),))

        self.sendMeaasgePlainTextEdit.textChanged.connect(self.changed)
        self.expand_flag= True
        self.contract_flag=True
        self.if_hide_all_3_on=False
        self.line_count=0
        self.max_line_count=20
        from PyQt5.QtGui import QTextCursor

        cursor= self.sendMeaasgePlainTextEdit.textCursor()
        cursor.movePosition(QTextCursor.Start)
        self.sendMeaasgePlainTextEdit.setTextCursor(cursor)

        self.plus_frame.hide()

        self.plus_pop_up_buttons_frame.hide()

        print(datetime.now())

        self.init_chat_theme_styleSheet()

        self.chat_seen_thread=None
        self.init_sticker()
        self.init_gif()
        self.hide_all_three_chat_area_floters_sticker_gif_plus_popup()

        self.gif_button.clicked.connect(self.gif_button_clicked)
        self.plus_gif_button.clicked.connect(self.gif_plus_button_clicked)
        self.plus_button.clicked.connect(self.plus_button_clicked)

        self.plus_sticker_button.clicked.connect(self.plus_sticker_button_clicked)
        self.sticker_button.clicked.connect(self.sticker_button_clicked)

        self.current_open_friend_email=None

        self.offline_color='''#9ca391'''
        self.online_color='''#00cf11'''
        self.local_last_seen_db=LastSeenLocalDB(db_path = "last_seen.db")
        self.last_seen_sync_thread=None

        self.sendMeaasgePlainTextEdit.installEventFilter(self)
        self.delete_frame_bg_label.installEventFilter(self)

        self.store_last_seen_cache=self.local_last_seen_db.read_all()

        self.ws_thread=LiveActivityWebSocketThread(self.current_user_email,current_mac_id)
        self.ws_thread.presence_update.connect(self.on_presence_update)
        self.ws_thread.connected.connect(self.on_ws_connected)
        self.ws_thread.disconnected.connect(lambda:print("presence socket down"))
        self.ws_thread.error.connect(lambda msg:print("ws error:",msg))

        self.ws_thread.new_message_received.connect(self.on_new_message_pushed)
        self.ws_thread.message_edited.connect(self.on_message_edited_pushed)
        self.ws_thread.message_deleted_all.connect(self.on_message_deleted_all_pushed)

        self.ws_thread.start()
        self.local_messages_db = MessagesLocalDB(db_path="messages.db")

        self.local_last_clicked_db=LastClickedLocalDB(db_path="last_clicked.db")
        self.last_clicked_cache=self.local_last_clicked_db.read_all()
        self._last_clicked_sent_snapshot={}

        self.ws_thread.connected.connect(self.flush_last_clicked_pending)
        self.ws_thread.sync_ack.connect(self.on_last_clicked_sync_ack)

        self.last_clicked_safety_net=LastClickedSafetyNetThread(flush_callback=self.flush_last_clicked_pending,interval_seconds=20,)
        self.last_clicked_safety_net.start()

        self.internet_thread=InternetMonitorThread(check_interval=2)

        self.internet_thread.went_offline.connect(self.on_offline)
        self.internet_thread.came_online.connect(self.on_online)

        self.internet_thread.start()

        self.mac_watcher=MacWatcherThread(self.current_user_email,current_mac_id)
        self.mac_watcher.mac_changed.connect(self.on_mac_changed)
        self.mac_watcher.start()

        self.chat_models={"first":QStandardItemModel(),"second":QStandardItemModel()}

        self.chat_list_view_1.setModel(self.chat_models["first"])
        self.chat_list_view_2.setModel(self.chat_models["second"])
        for m in self.chat_models.values():
            m.rowsInserted.connect(self._schedule_scroll)
            m.rowsRemoved.connect(self._schedule_scroll)
            m.dataChanged.connect(self._schedule_scroll)




        self.send_button.clicked.connect(self.send_button_clicked)
        self.delete_main_frame.hide()
        self.edit_prev_frame.hide()
        self.chat_list_view_2.hide()
        self.chat_list_view_1.hide()
        self.temp_label_for_deleting=None
        self.delete_frame_cancel_button.clicked.connect(self.delete_frame_cancel_button_clicked)
        self.non_curr_delete_frame_cancel_button.clicked.connect(self.non_curr_delete_frame_cancel_button_clicked)

        self.user_msg_to_delete= None
        self.delete_for_me_button.clicked.connect(self.delete_from_me_clicked)
        self.non_curr_delete_for_me_button.clicked.connect(self.delete_from_me_clicked)
        self.delete_for_all_button.clicked.connect(self.delete_for_all_button_clicked)

        self.password_msg_button.clicked.connect(self.password_msg_button_clicked)
        self.encrypt_text_plainTextEdit.textChanged.connect(self.encrypt_on_text_chnaged)

        self.encrypt_text_plainTextEdit.installEventFilter(self)
        self.encrypted_password_lineEdit.installEventFilter(self)
        self.max_lines_for_encrypt=15
        self.encrypt_cancel_button.clicked.connect(lambda:self.hide_and_clear_all_encrypt_all_stuff())

        self.view_view_button.clicked.connect(self.verify_decirptpassword_and_show_msg)
        self.temp_label_for_encryption=None
        self.view_encrypted_password_lineEdit.installEventFilter(self)
        self.password_protected_data_dict=None

        self.view_encrypted_cancel_button.clicked.connect(lambda:self.hide_and_clear_all_encrypt_all_stuff())
        self.encrypt_text_save_button.clicked.connect(lambda:self.check_for_ecryption_enter())

        self.encrypt_bg_label_main.installEventFilter(self)
        self.view_encrypt_bg_label_main.installEventFilter(self)
        self.hide_and_clear_all_encrypt_all_stuff()

        self.image_button.clicked.connect(self.image_button_clicked)
        self.plus_image_button.clicked.connect(self.image_button_clicked)

        self._preview_nam=QNetworkAccessManager(self)
        self._late_image_threads={}
        self._live_threads=set()
        self.user_messages_dict_and_list= {}
        self._chat_generation = 0

    def on_last_messages_preview_fetched(self,previews:dict):
        try:
            self.last_message_preview_db.upsert_many(previews)
            for friend_email,entry in previews.items():
                self.apply_last_message_preview(friend_email,entry)
            self._schedule_friend_reorder()
        except Exception:
            print('error in on_last_messages_preview_fetched:\n'+traceback.format_exc())


    
    def apply_last_message_preview(self, friend_email, entry):
        try:
            friend_entry = self.friend_list_left_label_dict.get(friend_email)
            if friend_entry is None or entry is None:
                return
    
            t_eff = msg_time(entry) or ''
            existing_last = friend_entry.get('last_msg_sent_at')
            if existing_last is not None and existing_last >= t_eff:
                return
    
            msg_label = friend_entry['label'].m_label
            time_labl = friend_entry['label'].t_label
            is_curr = entry.get('sent_by') == self.current_user_email
    
            self.set_text_to_stuff(time_labl,
                                   datetime.fromisoformat(t_eff).astimezone().strftime('%H:%M %p'))
    
            msg_type = entry.get('msg_type')
            if msg_type == 'text':
                for i in msg_label.findChildren(QWidget, options=Qt.FindDirectChildrenOnly):
                    i.deleteLater()
                content = (entry.get('msg_content') or '').replace('\n', ' ')
                self.set_text_to_stuff(msg_label, f'You : {content}' if is_curr else content)
            else:
                icon_map = {'sticker': ('svg_icons/sticker.svg', 'STICKER'), 'gif': ('svg_icons/gif.svg', 'GIF'),
                            'image': ('svg_icons/image.svg', 'Image'),
                            'encrypt': ('svg_icons/msg_lock.svg', 'Locked Message'),
                            'delete_from_everyone': ('svg_icons/delete.svg', 'Deleted Message'), }
                icon_path, label_text = icon_map.get(msg_type, ('svg_icons/chat_report.svg', msg_type or ''))
                self.render_friend_preview_media(msg_label, is_curr, icon_path, label_text)
    
            friend_entry['last_msg_sent_at'] = t_eff
            friend_entry['last_msg_id'] = entry.get('_id')
        except Exception:
            print('error in apply_last_message_preview:\n' + traceback.format_exc())

    
    def _init_chat_scroll(self):
        self._stick_bottom=True
        self._scroll_lock=False
        self._populating=False
        self._want_animate=False
        self._loading_older=False
        self._history_exhausted=False

        self._scroll_timer=QTimer(self)
        self._scroll_timer.setSingleShot(True)
        self._scroll_timer.timeout.connect(self._do_scroll_bottom)

        self._scroll_anims={}
        for view in (self.chat_list_view_1,self.chat_list_view_2):
            sb=view.verticalScrollBar()
            anim=QPropertyAnimation(sb,b"value",self)
            anim.setDuration(180)
            anim.setEasingCurve(QEasingCurve.OutCubic)
            self._scroll_anims[view]=anim
            sb.valueChanged.connect(lambda v,vw=view:self._on_scroll_value(vw,v))

        self._spinner=LoadingSpinner(self)

    def _active_view(self):
        if self.current_central_chat_frame=='first':
            return self.chat_list_view_1
        if self.current_central_chat_frame=='second':
            return self.chat_list_view_2
        return None

    def _schedule_scroll(self,*_):
        if not self._populating:
            self._want_animate=True
        self._scroll_timer.start(0)

    def _do_scroll_bottom(self):
        animate,self._want_animate=self._want_animate ,False
        view=self._active_view()
        if view is None or self._scroll_lock or not self._stick_bottom:
            return
        view.doItemsLayout()
        sb= view.verticalScrollBar()
        anim=self._scroll_anims[view]
        anim.stop()
        gap=sb.maximum() - sb.value()
        if animate and 0 < gap < 1500:
            anim.setStartValue(sb.value())
            anim.setEndValue(sb.maximum())
            anim.start()
        else:
            sb.setValue(sb.maximum())

    def _on_scroll_value(self,view,value):
        if view is not self._active_view() or self._scroll_lock:
            return
        if self._scroll_anims[view].state()==QAbstractAnimation.Running:
            return
        sb=view.verticalScrollBar()
        self._stick_bottom=value>=sb.maximum() - 4
        if value==0 and sb.maximum() > 0 and self.current_open_friend_email:
            fe=self.current_open_friend_email
            QTimer.singleShot(0,lambda:self.on_scrolled_to_top(fe))

    def _scroll_to_message(self,msg_id):
        view= self._active_view()
        model =self.chat_free_model
        for row in range(model.rowCount()):
            idx=model.index(row,0)
            w= view.indexWidget(idx)
            if w is not None and getattr(w,'_id',None) ==msg_id:
                view.scrollTo(idx,QAbstractItemView.PositionAtTop)
                return True

        return False

    def _set_loading(self,on):
        self._loading_older=on
        view=self._active_view()
        if on and view is not None:
            vp=view.viewport()
            self._spinner.setParent(vp)
            self._spinner.move((vp.width()-self._spinner.width())//2 ,12)
            self._spinner.start()
        else:
            self._spinner.stop()

    def _on_older_error(self,err,friend_email):
        print(f'fetch_older error: {err}')
        if friend_email==self.current_open_friend_email:
            self._set_loading(False)



    def _track_thread(self,thread):
        if not hasattr(self,'_live_threads'):
            self._live_threads=set()
        self._live_threads.add(thread)

        def _cleanup():
            self._live_threads.discard(thread)
            thread.deleteLater()

        thread.finished.connect(_cleanup)
        thread.start()
        return thread

    def _inflight(self):
        if not hasattr(self, '_sending_ids'):
            self._sending_ids = set()
        return self._sending_ids

    _WIRE_KEYS = ('_id', 'msg_type', 'msg_content', 'is_edited', 'sent_by', 'sent_to', 'sent_at', 'delete_from_me',
                  'delete_from_all', 'thumbnail_url', 'width', 'height')

    @staticmethod
    def _wire_payload(m):
        out = {k: m.get(k) for k in chating_main._WIRE_KEYS}
        for k in ('is_edited', 'delete_from_me', 'delete_from_all'):
            out[k] = bool(out[k])
        return out

    def _send_to_server(self, payload):
        msg_id = payload.get('_id')
        inflight = self._inflight()
        if msg_id in inflight:
            return
        inflight.add(msg_id)
        self.local_messages_db.mark_unsynced(msg_id)
        t = SendMessageThread(payload)
        t.finished_ok.connect(lambda resp, mid=msg_id: self._on_message_sent(mid, resp))
        t.error.connect(lambda e, mid=msg_id: self._on_message_send_failed(mid, e))
        self._track_thread(t)
    
    def _on_message_sent(self, msg_id, resp=None):
        self._inflight().discard(msg_id)
        self.local_messages_db.mark_synced(msg_id)
        # server-stamped delivery time replaces the local placeholder (received_at == sent_at)
        if resp and resp.get('received_at'):
            self.local_messages_db.set_received_at(msg_id, resp['received_at'])
    def _on_message_send_failed(self, msg_id, err):
        print(f'send failed ({msg_id}): {err}')
        self._inflight().discard(msg_id)
        self._schedule_unsent_retry()

    def _schedule_unsent_retry(self, delay_ms=10000):
        if getattr(self, '_unsent_retry_pending', False):
            return
        self._unsent_retry_pending = True
        QTimer.singleShot(delay_ms, self._run_unsent_retry)

    def _run_unsent_retry(self):
        self._unsent_retry_pending = False
        self.flush_unsent_messages()

    def flush_unsent_messages(self):
        try:
            pending = self.local_messages_db.get_unsynced(self.current_user_email)
            for m in pending:
                self._resend_message(m)
            if pending:
                # re-check later; stops by itself once everything is synced
                self._schedule_unsent_retry()
        except Exception:
            print('error in flush_unsent_messages:\n' + traceback.format_exc())

    def _resend_message(self, m):
        msg_id = m['_id']
        inflight = self._inflight()
        wire = self._wire_payload(m)

        if m.get('msg_type') == 'image':
            content = str(m.get('msg_content') or '')
            already_uploaded = content.startswith(('http://', 'https://')) and bool(m.get('thumbnail_url'))
            if not already_uploaded:
                local_path = m.get('local_image_path') or content
                if not local_path or not os.path.exists(local_path):
                    print(f'[resend] image file missing for {msg_id}')
                    return
                key = f'up_{msg_id}'
                if key in inflight:
                    return
                inflight.add(key)
                up = ImageUploadThread(local_path, msg_id)
                up.finished_ok.connect(
                    lambda full, thumb, p=wire, k=key: (
                        self._inflight().discard(k),
                        self._on_image_uploaded(p, full, thumb)))
                up.error.connect(
                    lambda e, k=key: (
                        self._inflight().discard(k),
                        print(f'[resend] image upload failed: {e}'),
                        self._schedule_unsent_retry()))
                self._track_thread(up)
                return

        self._send_to_server(wire)

    def sync_after_reconnect(self):
        # debounced: on_online and on_ws_connected both call this
        if getattr(self, '_catchup_pending', False):
            return
        self._catchup_pending = True
        QTimer.singleShot(1500, self._run_catchup)

    def _run_catchup(self):
        self._catchup_pending = False
        try:
            # 1) send everything queued while offline
            self.flush_unsent_messages()

            # 2) refresh left friend list (last sent/received per friend)
            t = FetchLastMessagesPreviewThread(self.current_user_email)
            t.finished_ok.connect(self.on_last_messages_preview_fetched)
            t.error.connect(lambda e: print(f'[catchup] preview failed: {e}'))
            self._track_thread(t)

            # 3) pull messages missed in the currently open chat
            fe = self.current_open_friend_email
            if fe:
                t2 = FetchOlderMessagesThread(self.current_user_email, fe, None, CHAT_PAGE_SIZE)
                t2.finished_ok.connect(lambda msgs, f=fe: self.on_catchup_messages(msgs, f))
                t2.error.connect(lambda e: print(f'[catchup] messages failed: {e}'))
                self._track_thread(t2)
        except Exception:
            print('error in _run_catchup:\n' + traceback.format_exc())

    def on_catchup_messages(self, msgs, friend_email):
        try:
            chat_id = get_local_chat_id(self.current_user_email, friend_email)
            need_rebuild = False

            for m in msgs:
                m = self._strip_local_fields(m)
                local = self.local_messages_db.get_by_id(m['_id'])

                # keep "delete for me" decisions (server tracks them in `deleted_for`)
                deleted_for = m.get('deleted_for') or []
                if self.current_user_email in deleted_for or (local and int(local.get('delete_from_me') or 0)):
                    m['delete_from_me'] = True

                hidden_for_me = bool(m.get('delete_from_me'))

                # a message we never rendered
                if not hidden_for_me and not self._widget_exists(m['_id']):
                    need_rebuild = True
                # deleted for everyone while we were offline
                if m.get('delete_from_all') and (local is None or not int(local.get('delete_from_all') or 0)):
                    need_rebuild = True

                self.local_messages_db.upsert_message(m, chat_id)
                # don't mark our own still-pending messages as synced
                if local is None or int(local.get('synced', 1)) != 0:
                    self.local_messages_db.mark_synced(m['_id'])


            if friend_email != self.current_open_friend_email or not need_rebuild:
                return

            loaded = self.chat_free_model.rowCount()
            limit = max(CHAT_PAGE_SIZE, loaded + len(msgs))
            latest = self.local_messages_db.get_latest(chat_id, limit=limit)
            latest = [x for x in latest if not int(x.get('delete_from_me') or 0)]
            self._stick_bottom = True
            self.populate_chat_from_messages(latest)
        except Exception:
            print('error in on_catchup_messages:\n' + traceback.format_exc())

    def _widget_exists(self,msg_id):
        model=getattr(self,'chat_free_model',None)
        if model is None or self.current_central_chat_frame is None or not msg_id:
            return False
        list_view=self.chat_list_view_1 if self.current_central_chat_frame=='first' else self.chat_list_view_2
        for row in range(model.rowCount()):
            item=model.item(row)
            if item is None:
                continue
            w=list_view.indexWidget(model.indexFromItem(item))
            if w is not None and getattr(w,'_id',None)==msg_id:
                return True
        return False

    @staticmethod
    def _strip_local_fields(msg):
        m=dict(msg)
        m.pop('local_thumbnail_path',None)
        m.pop('local_image_path',None)
        return m

    def create_friend_list_label(self,data):
        main_button=QPushButton(self)
        main_button.setFixedSize(335 , 71)
        main_button.setStyleSheet("""QPushButton { background-color:#fafafa;border-radius:10px;border:none;} QPushButton:hover { background-color:#e8eef5;} QPushButton:pressed { background-color:#dde6ef;} QPushButton:checked { background-color:#dbe7f3;} QPushButton:checked:hover { background-color:#d2e0ee;}""")
        main_button.setCursor(Qt.PointingHandCursor)

        pfp_img=QLabel(main_button)
        pfp_img.setGeometry(10,12,48,48)
        pfp_img.setStyleSheet('background:transparent;')
        self.set_circular_image(pfp_img ,data['pfp_location'])
        name_label = QLabel(main_button)
        name_label.setGeometry(65,15,211,21)
        name_label.setStyleSheet("""QLabel { font-family:"Inter","Segoe UI";font-size:17px;font-weight:550;color:#1E1E1E;background:transparent;}""")
        self.set_text_to_stuff(name_label,data['name'])

        msg_label=QLabel(main_button)
        msg_label.setGeometry(65,36,250,30)
        msg_label.setStyleSheet("""QLabel { font-family:"Inter","Segoe UI";font-size: 14px;font-weight:400;color:rgb(182,182,182);background:transparent;}""")
        msg_label.setAlignment(Qt.AlignTop)

        self.set_text_to_stuff(msg_label,'shgfaj fbgf fn hfdn gnfd mnf gmf gnm sdmn fnas gnfd anmg dfnmr n')

        time_label=QLabel(main_button)
        time_label.setGeometry(270,15,51,21)
        time_label.setStyleSheet('background:transparent;')
        time_label.setText('15:00 PM')

        main_button.show()
        main_button.m_label=msg_label
        main_button.t_label=time_label
        main_button.clicked.connect(lambda:self.friend_chat_button_clicked(data['email'],data))
        self.friend_list_left_label_dict[data['email']]=FriendEntry({"label":main_button,"last_msg_sent_at":None,"last_msg_id":None},on_time_changed=self._schedule_friend_reorder)
        return main_button

    def _has_day_label(self,date_str,model=None,list_view=None):
        if model is None:
            model= self.chat_free_model
        if list_view is None:
            list_view=self.chat_list_view_1 if self.current_central_chat_frame=='first' else self.chat_list_view_2

        for row in range(model.rowCount()):
            item=model.item(row)
            index=model.indexFromItem(item)
            widget=list_view.indexWidget(index)
            if widget is not None and getattr(widget,'_is_day_label',False) and getattr(widget,'_day_str',None)==date_str:
                return True

        return False

    def create_top_day_time_label(self,date_str):
        theme=self.current_chat_theme_styleSheet
        max_main_bg_w=1100

        label=QLabel()
        label.setStyleSheet(theme['time_stylesheet'])
        label.setAlignment(Qt.AlignCenter)

        d=datetime.strptime(date_str,"%Y-%m-%d").date()
        today=datetime.now().astimezone().date()
        if d==today:
            text="Today"
        elif d==today - timedelta(days=1):
            text="Yesterday"
        elif d.year==today.year:
            text=d.strftime("%B %d")
        else:
            text= d.strftime("%B %d, %Y")

        label.setText(text)
        label.adjustSize()

        wrapper=QLabel()
        wrapper.setStyleSheet('background:transparent;')
        wrapper.setFixedSize(max_main_bg_w , label.height()+10)
        label.setParent(wrapper)
        label.move((max_main_bg_w-label.width())//2,5)
        label.show()

        wrapper._is_day_label=True
        wrapper._day_str=date_str

        return wrapper

    def group_messages_by_date(self,messages):
        grouped={}
        for m in messages:
            dt=datetime.fromisoformat(m['sent_at']).astimezone()
            key=dt.strftime("%Y-%m-%d")
            grouped.setdefault(key,[]).append(m)

        for key in grouped:
            grouped[key].sort(key=lambda x:x['sent_at'])

        grouped=dict(sorted(grouped.items() ,key=lambda kv:kv[0]))
        return grouped

    def populate_chat_from_messages(self,messages,anchor_id=None):
        for a in self._scroll_anims.values():
            a.stop()
        self._scroll_lock=True
        self._populating=True
        try:
            self._populate_impl(messages)
        finally:
            self._populating=False
            self._scroll_timer.stop()
            view=self._active_view()
            if view is not None:
                view.doItemsLayout()
                sb=view.verticalScrollBar()
                if anchor_id and self._scroll_to_message(anchor_id):
                    self._stick_bottom=sb.value() >= sb.maximum() - 4
                else:
                    self._stick_bottom=True
                    sb.setValue(sb.maximum())
            self._scroll_lock=False


    
    def _populate_impl(self, messages):
        try:
            key = 'first' if self.current_central_chat_frame == 'first' else 'second'
            self.clear_chat_model(key)
    
            seen = set()
            deduped = []
            for m in messages:
                mid = m.get('_id')
                if mid in seen:
                    continue
                seen.add(mid)
                deduped.append(m)
    
            deduped.sort(key=lambda x: msg_time(x) or '')
    
            image_msgs = []
            self._suspend_day_rebuild = True
            try:
                for m in deduped:
                    is_mine = m.get('sent_by') == self.current_user_email
    
                    if is_mine and int(m.get('delete_from_me') or 0):
                        continue
    
                    if int(m.get('delete_from_all') or 0):
                        self.push_a_single_delete_from_everyone_direct(m)
                        continue
    
                    mtype = m.get('msg_type')
                    if mtype == 'text':
                        self.push_a_text_user_label(m)
                    elif mtype == 'sticker':
                        self.push_a_single_sticker(m)
                    elif mtype == 'gif':
                        self.push_a_single_gif(m)
                    elif mtype == 'encrypt':
                        self.push_a_single_locked_msg(m)
                    elif mtype == 'delete_from_everyone':
                        self.push_a_single_delete_from_everyone_direct(m)
                    elif mtype == 'image':
                        image_msgs.append(m)
            finally:
                self._suspend_day_rebuild = False
    
            self.rebuild_day_labels()
    
            for m in image_msgs:
                self.resolve_and_show_image(m)
        except Exception:
            print('error in _populate_impl:\n' + traceback.format_exc())

    def build_demo_messages(self,friend_email):
        now=datetime.now(timezone.utc)

        samples=[(now-timedelta(days=2,hours=3),self.current_user_email,friend_email,"Hey, how's it going?","text"),(now-timedelta(days=2,hours=2,minutes=58),friend_email,self.current_user_email,"Pretty good! You?","text"),(now-timedelta(days=1,hours=5),self.current_user_email ,friend_email,"Did you see the game last night?","text"),(now-timedelta(days=1,hours = 4,minutes=50),friend_email,self.current_user_email,"Yeah insane finish","text"),(now-timedelta(hours= 2),friend_email,self.current_user_email,"gAAAAABqp3_97NkifE5Pv2uSFxr5GSkVMX9d-VthCm1351Ocq2yvv40ja22MXoeX08WVTj8rx76UY4YFfIqJ9oo9-LGjEEhF3w==","encrypt") ,(now-timedelta(hours= 1,minutes=55), friend_email,self.current_user_email,"my_gif/anime0_sinchan/imgi_70_sticker_16.gif","gif"),(now-timedelta(minutes=40),self.current_user_email,friend_email,"Still up for lunch tomorrow?","text"),(now-timedelta(minutes=35),friend_email,self.current_user_email,"Yep, same place as always","text"),]

        messages=[]

        for sent_at,sent_by,sent_to,content,msg_type in samples:
            messages.append({'_id':self.generate_random_id(),'msg_type':msg_type,'msg_content':content,'sent_by':sent_by,'sent_to':sent_to,'sent_at':sent_at.isoformat(),'delete_from_me' : False,'delete_from_all':False})

        return messages

    def save_image_with_thumbnail(self,file_path,id1 , quality=92):
        import io,urllib.request
        from PIL import Image
        thumbnail_folder="thumbnail_images"
        user_images_folder="user_images"
        os.makedirs(thumbnail_folder,exist_ok=True)
        os.makedirs(user_images_folder,exist_ok=True)
        is_url=isinstance(file_path,str) and file_path.lower().startswith(("http://","https://"))
        ext=os.path.splitext(file_path.split("?")[0]if is_url else file_path)[1].lower()
        ext=ext if ext in (".png",".jpg",".jpeg") else ".jpg"
        thumbnail_path=os.path.join(thumbnail_folder,f"{id1}_thumbnail.jpg")
        user_image_path=os.path.join(user_images_folder,f"{id1}{ext}")
        try:
            raw_bytes=urllib.request.urlopen(urllib.request.Request(file_path, headers={"User-Agent": "Mozilla/5.0"}),timeout=10).read() if is_url else open(file_path,"rb").read()
        except Exception as e:
            print(f"Failed to load image from source: {e}")
            return None,None,None,None
        try:
            open(user_image_path,"wb").write(raw_bytes)
        except Exception as e:
            print(f"Failed to save original image: {e}")
            return None,None,None,None
        try:
            pil_image=Image.open(io.BytesIO(raw_bytes))
            pil_image=pil_image.convert("RGB") if pil_image.mode!="RGB" else pil_image

            orig_w,orig_h=pil_image.width,pil_image.height

            max_dim=1600
            if pil_image.width > max_dim or pil_image.height > max_dim:
                pil_image.thumbnail((max_dim,max_dim),Image.LANCZOS)

            pil_image.save(thumbnail_path,"JPEG",quality=quality,optimize=True,subsampling=0)
        except Exception as e:
            print(f"Failed to create compressed image: {e}")
            return None,user_image_path,None,None
        return thumbnail_path,user_image_path,orig_w,orig_h

    def image_button_clicked(self):
        self.hide_all_three_chat_area_floters_sticker_gif_plus_popup()
        from PyQt5.QtWidgets import QFileDialog,QMessageBox
        file_path,_=QFileDialog.getOpenFileName(self ,"Select Image","","Images (*.png *.jpg *.jpeg)")
        if not file_path: return
        if not file_path.lower().endswith((".png",".jpg",".jpeg")): QMessageBox.warning(self,"Invalid File","Only JPG and PNG files are allowed."); return
        if os.path.getsize(file_path) / (1024*1024) > 5: QMessageBox.warning(self,"File Too Large","Maximum file size is 5MB."); return
        id1=self.generate_random_id()
        t=ImageSaveThread(self.save_image_with_thumbnail,file_path,id1,quality=92)
        t.finished_ok.connect(self.on_image_saved)
        t.finished_error.connect(self.on_image_save_error)
        self._track_thread(t)


    
    def on_image_saved(self, thumbnail_path, user_image_path, id1, width, height):
        try:
            self._stick_bottom = True
            ts = datetime.now(timezone.utc).isoformat()
            payload = {'_id': id1, 'msg_type': 'image', 'msg_content': user_image_path,
                       'local_thumbnail_path': thumbnail_path, 'local_image_path': user_image_path,
                       'thumbnail_url': None, 'is_edited': False, 'sent_by': self.current_user_email,
                       'sent_to': self.user_is_on_freind, 'sent_at': ts, 'received_at': ts,
                       'delete_from_me': False, 'delete_from_all': False, 'width': width, 'height': height}
            self.push_a_single_image_user_label(payload, insert_sorted=False, ensure_today=True)
            chat_id = get_local_chat_id(self.current_user_email, payload['sent_to'])
            self.local_messages_db.upsert_message(payload, chat_id)
            self.local_messages_db.mark_unsynced(id1)
    
            key = f'up_{id1}'
            self._inflight().add(key)
            up = ImageUploadThread(user_image_path, id1)
            up.finished_ok.connect(
                lambda full_url, thumb_url, p=payload, k=key: (
                    self._inflight().discard(k),
                    self._on_image_uploaded(p, full_url, thumb_url)))
            up.error.connect(
                lambda e, k=key: (
                    self._inflight().discard(k),
                    print(f'image upload failed: {e}'),
                    self._schedule_unsent_retry()))
            self._track_thread(up)
        except Exception:
            print('error in on_image_saved:\n' + traceback.format_exc())






    def _on_image_uploaded(self,payload,full_url ,thumb_url):
        chat_id =get_local_chat_id(payload['sent_by'],payload['sent_to'])
        updated={**payload,'msg_content':full_url,'thumbnail_url':thumb_url}
        self.local_messages_db.upsert_message(updated ,chat_id)
        self._send_to_server(self._strip_local_fields(updated))

    def on_image_save_error(self,msg):
        from PyQt5.QtWidgets import QMessageBox
        QMessageBox.warning(self,"Thumbnail Error",msg)

    def get_image_size(self,width,height):
        min_height=100
        max_height=350
        min_width=100
        max_width=500

        aspect_ratio=width / height

        scale=min(max_width/width,max_height/height)

        new_width=width * scale
        new_height=height * scale

        if new_width < min_width:
            scale=min_width / width
            new_width =width * scale
            new_height=height * scale

        if new_height < min_height:
            scale=min_height / height
            new_width=width * scale
            new_height=height * scale

        return round(new_width),round(new_height)



    def _image_local_paths(self,data):
        msg_id=data.get('_id')
        thumb_path=data.get('local_thumbnail_path') or os.path.join("thumbnail_images" ,f"{msg_id}_thumbnail.jpg")
        full_path=data.get('local_image_path') or os.path.join("user_images",f"{msg_id}.jpg")
        return thumb_path , full_path

    def resolve_and_show_image(self,data):
        try:
            thumb_path,full_path=self._image_local_paths(data)

            if os.path.exists(thumb_path):
                data=dict(data)
                data['local_thumbnail_path']=thumb_path
                if os.path.exists(full_path):
                    data['local_image_path']=full_path
                self.push_a_single_image_user_label(data,insert_sorted=True)
                return

            self._ensure_image_download(data)
        except Exception:
            print('error in resolve_and_show_image:\n'+traceback.format_exc())

    def _ensure_image_download(self,data):
        msg_id= data.get('_id')
        if not hasattr(self,'_late_image_threads'):
            self._late_image_threads={}
        if msg_id in self._late_image_threads:
            return

        thumbnail_url=data.get('thumbnail_url')
        full_url=data.get('msg_content')
        if not thumbnail_url or not full_url or not str(full_url).startswith(('http://','https://')):
            return

        sent_by=data.get('sent_by','')
        sent_to=data.get('sent_to') or (self.user_is_on_freind if sent_by==self.current_user_email else self.current_user_email)
        safe_data={**data,'sent_by':sent_by,'sent_to':sent_to}

        thread=ImageDownloadThread(thumbnail_url,full_url,msg_id)
        thread.finished_ok.connect(lambda thumb_path,full_path,d= safe_data:self._on_image_downloaded(d,thumb_path,full_path))
        thread.finished_error.connect(lambda e,mid=msg_id:(print(f'[image download] failed {mid}: {e}') ,self._late_image_threads.pop(mid,None)))
        self._late_image_threads[msg_id]=thread
        self._track_thread(thread)

    def _on_image_downloaded(self,data,thumb_path,full_path,generation=None):
        try:
            msg_id=data.get('_id')
            self._late_image_threads.pop(msg_id,None)

            sent_by,sent_to=data['sent_by'],data['sent_to']
            chat_id=get_local_chat_id(sent_by,sent_to)
            updated={**data,'local_thumbnail_path':thumb_path,'local_image_path':full_path}
            self.local_messages_db.upsert_message(updated,chat_id)

            other=sent_to if sent_by==self.current_user_email else sent_by
            if other!=self.current_open_friend_email:
                return
            if self._widget_exists(msg_id):
                return

            self.push_a_single_image_user_label(updated,insert_sorted=True)
        except Exception:
            print('error in _on_image_downloaded:\n'+traceback.format_exc())

    def insert_message_widget_sorted(self , label,sent_at):
        try:
            list_view=self.chat_list_view_1 if self.current_central_chat_frame=='first' else self.chat_list_view_2
            model=self.chat_free_model
            insert_row=model.rowCount()
            for row in range(model.rowCount()):
                item=model.item(row)
                index = model.indexFromItem(item)
                widget=list_view.indexWidget(index)
                t=getattr(widget,'_time',None)
                if t is None:
                    continue
                if t > sent_at:
                    insert_row=row
                    break
            item=QStandardItem()
            size=label.sizeHint()
            size.setHeight(size.height()+10)
            item.setSizeHint(size)
            model.insertRow(insert_row,item)
            index=model.indexFromItem(item)
            list_view.setIndexWidget(index,label)
            self.rebuild_day_labels()
            return index
        except Exception:
            print('error in insert_message_widget_sorted:\n'+traceback.format_exc())




    def on_message_edited_pushed(self,msg_id,new_content):
        if self.current_central_chat_frame=='first':
            list_view=self.chat_list_view_1
        else:
            list_view=self.chat_list_view_2

        for row in range(self.chat_free_model.rowCount()):
            item=self.chat_free_model.item(row)
            index=self.chat_free_model.indexFromItem(item)
            widget=list_view.indexWidget(index)
            if widget is not None and getattr(widget,'_id',None)==msg_id:
                sent_by=getattr(widget,'_sent_by')
                sent_at=getattr(widget,'_time')
                sent_to=self.current_user_email if sent_by!=self.current_user_email else self.user_is_on_freind
                data={'_id':msg_id,'msg_type':'text','msg_content':new_content,'is_edited':True,'sent_by':sent_by,'sent_at':sent_at,'sent_to':sent_to,'delete_from_me':False ,'delete_from_all' : False}
                new_label=self.create_text_msg_label(data)
                if new_label is not None:
                    old_widget=list_view.indexWidget(index)
                    if old_widget is not None:
                        old_widget.deleteLater()
                    size=new_label.sizeHint()
                    size.setHeight(size.height()+10)
                    self.chat_free_model.itemFromIndex(index).setSizeHint(size)
                    list_view.setIndexWidget(index,new_label)

                    chat_id= get_local_chat_id(sent_by,sent_to)
                    self.local_messages_db.upsert_message(data,chat_id)
                break



    def push_a_single_image_user_label(self, data, insert_sorted=True, ensure_today=False):
        try:
            data = self._view(data)
            if self._widget_exists(data.get('_id')):
                return
            if ensure_today:
                self.ensure_day_label_for_today()
            label = self.create_image_msg_label(data)
            if label is None:
                return
            if insert_sorted:
                self.insert_message_widget_sorted(label, data.get('sent_at'))
            else:
                self.push_image_label_in_chat(label)
        except Exception:
            print('error in push_a_single_image_user_label:\n' + traceback.format_exc())



    
    def push_image_label_in_chat(self,label):
        try:
            item=QStandardItem()
            size=label.sizeHint()
            size.setHeight(size.height()+10)
            item.setSizeHint(size)
            self.chat_free_model.appendRow(item)
            index =self.chat_free_model.indexFromItem(item)
            if self.current_central_chat_frame=='first':
                self.chat_list_view_1.setIndexWidget(index,label)
            else:
                self.chat_list_view_2.setIndexWidget(index,label)
            self.rebuild_day_labels()
        except Exception:
            print('error in push_image_label_in_chat:\n'+traceback.format_exc())



    def _render_pixmap_into_image_label(self,image_label,raw_pixmap,w,h):
        try:
            dpr=image_label.devicePixelRatioF() if hasattr(image_label,'devicePixelRatioF') else 1.0
            target_px_w= max(1,int(w*dpr))
            target_px_h=max(1,int(h*dpr))
            if raw_pixmap.isNull():
                raw_pixmap=QPixmap(w,h)
                raw_pixmap.fill(QColor(60,60,60))
            scaled=raw_pixmap.scaled(target_px_w,target_px_h,Qt.KeepAspectRatioByExpanding,Qt.SmoothTransformation)
            crop_x=max(0,(scaled.width()-target_px_w)//2)
            crop_y=max(0,(scaled.height()-target_px_h)//2)
            rounded=QPixmap(target_px_w,target_px_h)
            rounded.setDevicePixelRatio(dpr)
            rounded.fill(Qt.transparent)
            painter=QPainter(rounded)
            painter.setRenderHint(QPainter.Antialiasing)
            painter.setRenderHint(QPainter.SmoothPixmapTransform)
            radius=min(w,h) * 0.06
            path=QPainterPath()
            path.addRoundedRect(QRectF(0,0,w,h),radius,radius)
            painter.setClipPath(path)
            painter.drawPixmap(0,0,w,h,scaled,crop_x,crop_y,target_px_w,target_px_h)
            painter.end()
            image_label.setPixmap(rounded)
        except (RuntimeError,Exception):

            pass

    def create_image_msg_label(self,data):
        try:
            from PyQt5.QtGui import QPainterPath,QRegion
            from PyQt5.QtCore import QRectF
            max_main_bg_w=1100
            padding=5
            extra_height_for_time=10
            theme=self.current_chat_theme_styleSheet

            thumb_path=data.get('local_thumbnail_path')
            has_local_thumb=bool(thumb_path and os.path.exists(thumb_path))

            stored_w=data.get('width')
            stored_h=data.get('height')

            if stored_w and stored_h:
                (width,height)=(stored_w , stored_h)
            elif has_local_thumb:
                image=QImage(thumb_path)
                width=image.width()
                height=image.height()
            else:
                (width,height)=(300,200)

            (w,h)=self.get_image_size(width,height)

            transparent_long_bg=QLabel()
            transparent_long_bg.setStyleSheet('background:transparent;')
            is_curr = self.current_user_email==data.get('sent_by',False)

            if is_curr:
                dict_target_email=data.get('sent_to','')
            else:
                dict_target_email=data.get('sent_by','')

            friend_entry=self.friend_list_left_label_dict.get(dict_target_email)

            if not data.get('is_temp',False) and friend_entry is not None:
                last_time=friend_entry.get('last_msg_sent_at')

                def _update_friend_preview():
                    try:
                        msg_label=friend_entry['label'].m_label
                        time_labl=friend_entry['label'].t_label
                        self.set_text_to_stuff(time_labl,datetime.fromisoformat(data['sent_at']).astimezone().strftime('%H:%M %p'))
                        msg_label.clear()
                        for i in msg_label.findChildren(QWidget,options=Qt.FindDirectChildrenOnly):
                            i.deleteLater()

                        s_o_s=14
                        if is_curr:
                            x1__=38
                            x2__=s_o_s + 2 + x1__
                            i_fall=QLabel(msg_label)
                            i_fall.setStyleSheet('font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;')
                            i_fall.setText('You : ')
                            i_fall.move(3,0)
                            i_fall.adjustSize()
                            i_fall.show()
                        else:
                            x1__=2
                            x2__= s_o_s + 2 + x1__
                            msg_label.setText('')

                        icon_lbl=QLabel(msg_label)
                        icon_lbl.setFixedSize(s_o_s,s_o_s)
                        icon_lbl.move(x1__,3)
                        icon_lbl.raise_()
                        icon_lbl.show()
                        self.set_svg_icon_and_color(icon_lbl,'svg_icons/image.svg','#b6b6b6')

                        txt_lbl =QLabel(msg_label)
                        txt_lbl.setStyleSheet('font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;')
                        txt_lbl.setText('Image')
                        txt_lbl.move(x2__,0)
                        txt_lbl.adjustSize()
                        txt_lbl.show()

                        friend_entry['last_msg_sent_at']=data['sent_at']
                        friend_entry['last_msg_id']=data['_id']
                    except Exception:
                        print('error in _update_friend_preview:\n'+traceback.format_exc())

                if last_time is None or last_time < data['sent_at']:
                    _update_friend_preview()

            maiN_image_bg_label=QLabel()
            image_label=QLabel(maiN_image_bg_label)
            image_label.setFixedSize(w,h)
            image_label.setScaledContents(False)

            if is_curr:
                main_x=max_main_bg_w - w - 20 - padding * 2
                time_ss=theme['right_text_time_stylesheet']
                bg_ss=f"background-color:{theme['current_user_chat_color']};border-radius:10px;"
            else:
                main_x=10
                time_ss=theme['left_text_time_stylesheet']
                bg_ss=f"background-color:{theme['receiver_user_chat_color']};border-radius:10px;"

            main_w__=w + padding * 2
            main_h__=h + padding * 5
            maiN_image_bg_label.setGeometry(main_x,0 ,main_w__,main_h__)
            maiN_image_bg_label.setStyleSheet(bg_ss)

            time_label=QLabel(maiN_image_bg_label)
            time_x=int(maiN_image_bg_label.width()-40-10)
            time_y=int(maiN_image_bg_label.height()-extra_height_for_time*1.65)
            time_label.move(time_x,time_y)
            local_time=datetime.fromisoformat(data['sent_at']).astimezone()
            time_label.setStyleSheet(time_ss)
            time_label.setText(f"{local_time.strftime('%I:%M %p')}")

            if has_local_thumb:
                raw_pixmap=QPixmap(thumb_path)
            else:
                raw_pixmap=QPixmap()

            self._render_pixmap_into_image_label(image_label,raw_pixmap,w,h)

            image_label.move(padding,padding)
            image_label.show()

            image_label._msg_id_for_late_load=data.get('_id')
            image_label._full_image_path=data.get('local_image_path') or data.get('msg_content')
            image_label._is_ready=has_local_thumb

            def image_label_clicked(event):
                try:
                    if not image_label._is_ready:
                        return
                    print(f'[image click] full image path = {image_label._full_image_path}')
                except Exception:
                    print('error in image_label_clicked:\n'+traceback.format_exc())

            image_label.mousePressEvent=image_label_clicked
            image_label.setCursor(Qt.PointingHandCursor if has_local_thumb else Qt.ForbiddenCursor)

            if data.get('is_temp',False):
                return maiN_image_bg_label

            transparent_long_bg.setFixedSize(max_main_bg_w,maiN_image_bg_label.height())
            maiN_image_bg_label.setParent(transparent_long_bg)
            maiN_image_bg_label.move(main_x,0)
            transparent_long_bg.setFixedSize(max_main_bg_w,maiN_image_bg_label.height())

            button_size=30
            button_gap=3
            y_of_button=int((transparent_long_bg.height()-button_size)//2)

            if is_curr:
                button_x=main_x - int(button_size*4.5+button_gap*3)
                report_x=button_x
                delete_x=report_x + button_size + button_gap
                edit_x=delete_x + button_size + button_gap
                copy_x=edit_x + button_size + button_gap
            else:
                button_x=maiN_image_bg_label.width() + int(button_size//2+button_gap)
                copy_x=button_x
                edit_x=copy_x + button_size + button_gap
                delete_x=edit_x + button_size + button_gap
                report_x=delete_x + button_size + button_gap

            report_button=QPushButton(transparent_long_bg)
            report_button.setGeometry(report_x,y_of_button,button_size,button_size)
            report_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(report_button,'svg_icons/chat_report.svg',self.current_chat_theme_styleSheet['chat_buttons_color'])
            report_button.setIconSize(QSize(int(button_size*0.5),int(button_size*0.5)))
            report_button.setCursor(Qt.PointingHandCursor)

            copy_button=QPushButton(transparent_long_bg)
            copy_button.setGeometry(copy_x,y_of_button,button_size,button_size)
            copy_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(copy_button,'svg_icons/chat_copy.svg',self.current_chat_theme_styleSheet['chat_buttons_color'],int(button_size*0.6))
            copy_button.setIconSize(QSize(int(button_size*0.6),int(button_size*0.6)))
            copy_button.setCursor(Qt.PointingHandCursor)

            delete_button=QPushButton(transparent_long_bg)
            delete_button.setGeometry(delete_x,y_of_button,button_size,button_size)
            delete_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(delete_button,'svg_icons/chat_bin2.svg',self.current_chat_theme_styleSheet['chat_buttons_color'],int(button_size*0.7))
            delete_button.setIconSize(QSize(int(button_size*0.7),int(button_size*0.7)))
            delete_button.setCursor(Qt.PointingHandCursor)

            edit_button=QPushButton(transparent_long_bg)
            edit_button.setGeometry(edit_x,y_of_button,button_size,button_size)
            edit_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(edit_button,'svg_icons/edit.svg',self.current_chat_theme_styleSheet['chat_buttons_color'],int(button_size*0.6))
            edit_button.setIconSize(QSize(int(button_size*0.7),int(button_size*0.7)))
            edit_button.setCursor(Qt.PointingHandCursor)

            report_button.hide()
            copy_button.hide()
            delete_button.hide()
            edit_button.hide()

            def show_inside_buttons(event):
                try:
                    report_button.show()
                    copy_button.show()
                    delete_button.show()
                    edit_button.show()
                except Exception:
                    print('error in show_inside_buttons:\n'+traceback.format_exc())

            def hide_inside_buttons(event):
                try:
                    report_button.hide()
                    copy_button.hide()
                    delete_button.hide()
                    edit_button.hide()
                except Exception:
                    print('error in hide_inside_buttons:\n'+traceback.format_exc())

            transparent_long_bg.enterEvent=show_inside_buttons
            transparent_long_bg.leaveEvent=hide_inside_buttons
            transparent_long_bg._id=data['_id']
            transparent_long_bg._type=data['msg_type']
            transparent_long_bg._msg_content=data['msg_content']
            transparent_long_bg._time=data['sent_at']
            transparent_long_bg._sent_by=data['sent_by']

            delete_button.clicked.connect(lambda:self.delete_a_chat_widget(data))

            return transparent_long_bg

        except Exception:
            print('error in create_image_msg_label:\n'+traceback.format_exc())
            return None

    def push_a_single_delete_from_everyone_via_id(self,data):
        self.ensure_day_label_for_today()

        data['msg_type']='delete_from_everyone'
        data['delete_from_all']=True
        data['is_temp']=False
        new_label=self.create_delete_from_every_one_label(data)

        if self.current_central_chat_frame=='first':
            list_view =self.chat_list_view_1
        else:
            list_view=self.chat_list_view_2

        target_index=None
        for row in range(self.chat_free_model.rowCount()):
            item=self.chat_free_model.item(row)
            index=self.chat_free_model.indexFromItem(item)
            widget=list_view.indexWidget(index)
            if widget is not None and getattr(widget,'_id',None)==data['_id']:
                target_index=index
                break


        if target_index is not None:
            old_widget=list_view.indexWidget(target_index)
            if old_widget is not None:
                old_widget.deleteLater()
            size=new_label.sizeHint()
            size.setHeight(size.height()+10)
            self.chat_free_model.itemFromIndex(target_index).setSizeHint(size)
            list_view.setIndexWidget(target_index,new_label)
            new_label._id=data['_id']


        sent_by=data.get('sent_by',self.current_user_email)
        sent_to=data.get('sent_to') or (self.user_is_on_freind if sent_by==self.current_user_email else self.current_user_email)
        chat_id=get_local_chat_id(sent_by,sent_to)
        self.local_messages_db.upsert_message({**data,'sent_by':sent_by,'sent_to':sent_to,'msg_content':data.get('msg_content','')} ,chat_id)

        notify_email=sent_to if sent_by==self.current_user_email else sent_by
        t=DeleteForAllThread(data['_id'],notify_email)
        t.error.connect(lambda e:print(f'delete_for_all failed: {e}'))
        self._track_thread(t)

        self.rebuild_day_labels()

    def clear_chat_model(self,key):
        try:
            self._chat_generation+=1
            model=self.chat_models[key]
            list_view=self.chat_list_view_1 if key=='first' else self.chat_list_view_2
            for i in range(model.rowCount()):
                item=model.item(i)
                idx=model.indexFromItem(item)
                label_thingy=list_view.indexWidget(idx)
                if label_thingy is not None:
                    label_thingy.deleteLater()

            model.clear()
        except Exception:
            print('error in clear_chat_model:\n'+traceback.format_exc())

    def verify_decirptpassword_and_show_msg(self):
        content_frame_base_x=580
        print('view button clicked')
        if self.view_encrypted_password_lineEdit.text().strip() == '':
            print('passowrd cant be empty')
            return
        verdict=self.decrypt_message_for_encipt_type(self.password_protected_data_dict['msg_content'],self.view_encrypted_password_lineEdit.text())
        if verdict is None:
            print(f'incorret password')
            return
        if self.temp_label_for_encryption is not None:
            self.temp_label_for_encryption.deleteLater()
            self.temp_label_for_encryption=None
        data=dict(self.password_protected_data_dict)
        data['is_temp']=True
        data['msg_content']=verdict
        self.temp_label_for_encryption=self.create_text_msg_label(data)
        x=20
        y=95
        self.temp_label_for_encryption.setParent(self.view_encrypt_white_bg_label)
        self.temp_label_for_encryption.move(x,y)
        self.temp_label_for_encryption.show()
        y2=y + self.temp_label_for_encryption.height()
        self.view_bottom_button_frame.move(x,y2)

        h=self.view_bottom_button_frame.y() + self.view_bottom_button_frame.height()
        self.view_encrypt_white_bg_label.setFixedSize(811,h)
        y=int((self.height()-h)//3.8)
        self.view_encrypt_content_frame.move(content_frame_base_x,y)
        self.view_encrypt_content_frame.setFixedSize(840,h+30)

    def encrypt_on_text_chnaged(self):
        try:
            content_frame_base_x=500
            edit=self.encrypt_text_plainTextEdit
            text=edit.toPlainText()
            fm=edit.fontMetrics()

            line_height= fm.lineSpacing()

            doc_margin=edit.document().documentMargin() * 2
            frame_width=edit.frameWidth() * 2
            available_width=int(edit.width()-doc_margin-frame_width)
            available_width=max(1,available_width)

            measure_text=text if text else " "

            bounding_rect=fm.boundingRect(QRect(0,0,available_width ,1_000_000),Qt.TextWordWrap|Qt.TextWrapAnywhere,measure_text)

            line_count=max(1,math.ceil(bounding_rect.height()/line_height))

            max_lines=self.max_lines_for_encrypt

            base_x=30
            base_y=110
            base_w=740
            base_h=40

            grow_lines=min(line_count-1,max_lines-1)
            delta=grow_lines * line_height

            new_h=base_h + delta

            edit.setGeometry(base_x,base_y,base_w,new_h)

            bottom_frame_x=30
            bottom_frame_y=base_y + new_h
            self.encrypt_bottom_frame.move(bottom_frame_x,bottom_frame_y)
            h=self.encrypt_bottom_frame.y() + self.encrypt_bottom_frame.height()
            self.encrypt_white_bg_label.setFixedSize(811,h)
            y=int((self.height()-h)//3.8)
            self.encrypt_content_frame.move(content_frame_base_x,y)

            self.encrypt_content_frame.setFixedSize(840,h+30)

        except Exception as e:
            print(e)

    def password_msg_button_clicked(self):
        if self.sendMeaasgePlainTextEdit.toPlainText().strip()!='':
            self.encrypt_text_plainTextEdit.setPlainText(self.sendMeaasgePlainTextEdit.toPlainText().strip())
        self.encrypt_msg_main_frame.show()
        self.sendMeaasgePlainTextEdit.moveCursor(QTextCursor.End)
        self.encrypt_text_plainTextEdit.setFocus()
        self.encrypted_password_lineEdit.clear()
        self.encrypt_msg_main_frame.move(0,0)

    def hide_and_clear_all_encrypt_all_stuff(self):
        self.encrypt_msg_main_frame.hide()
        self.view_encrypt_msg_main_frame.hide()
        self.view_bottom_button_frame.move(20,160)
        self.encrypt_text_plainTextEdit.setPlainText("")
        self.encrypt_on_text_chnaged()
        if self.temp_label_for_encryption is not None:
            self.temp_label_for_encryption.deleteLater()
            self.temp_label_for_encryption=None

    def check_for_ecryption_enter(self):
        if self.encrypt_text_plainTextEdit.toPlainText().strip()=='':
            self.encrypt_text_plainTextEdit.setFocus()
            return
        if self.encrypted_password_lineEdit.text().strip() == '':
            self.encrypted_password_lineEdit.setFocus()
            return
        self.encrypt_text_save_button_clicked()

    def encrypt_text_save_button_clicked(self):
        try:
            if self.encrypt_text_plainTextEdit.toPlainText().strip() == '':
                self.encrypt_text_plainTextEdit.setFocus()
                return
            if self.encrypted_password_lineEdit.text().strip() == '':
                self.encrypted_password_lineEdit.setFocus()
                return
            msg = self.encrypt_text_plainTextEdit.toPlainText().strip()
            password = self.encrypted_password_lineEdit.text().strip()
            self.hide_and_clear_all_encrypt_all_stuff()
            self._stick_bottom = True
            ts = datetime.now(timezone.utc).isoformat()
            payload = {'_id': self.generate_random_id(), 'msg_type': 'encrypt',
                       'msg_content': self.encrypt_message_for_encipt_type(msg, password), 'is_edited': False,
                       'sent_by': self.current_user_email, 'sent_at': ts, 'received_at': ts,
                       'sent_to': self.user_is_on_freind, 'delete_from_me': False, 'delete_from_all': False}
            self.push_a_single_locked_msg(payload)
            chat_id = get_local_chat_id(self.current_user_email, self.user_is_on_freind)
            self.local_messages_db.upsert_message(payload, chat_id)
            self._send_to_server(payload)
        except Exception:
            print('error in encrypt_text_save_button_clicked:\n' + traceback.format_exc())

    def encrypt_message_for_encipt_type(self ,msg,password):
        key=base64.urlsafe_b64encode(hashlib.sha256(password.encode("utf-8")).digest())

        cipher=Fernet(key)

        encrypted=cipher.encrypt(msg.encode("utf-8"))

        return encrypted.decode("utf-8")

    def decrypt_message_for_encipt_type(self,encrypted_text,password):
        try:
            key=base64.urlsafe_b64encode(hashlib.sha256(password.encode("utf-8")).digest())

            cipher=Fernet(key)

            decrypted=cipher.decrypt(encrypted_text.encode("utf-8"))

            return decrypted.decode("utf-8")

        except Exception:
            return None

    def schedule_temp_label(self ,temp_data_for_label):
        QTimer.singleShot(10,lambda:self.one(temp_data_for_label))

    def ensure_day_label_for_today(self):
        return

    def remove_orphaned_day_labels(self):
        self.rebuild_day_labels()
    def _view(self, m):
        """Copy of a message where 'sent_at' holds the effective time (received_at, else sent_at)."""
        d = dict(m)
        eff = msg_time(d)
        d['received_at'] = eff
        d['sent_at'] = eff
        return d
    def ensure_day_label_for_today(self):
        today_str=datetime.now().strftime("%Y-%m-%d")

        if self._has_day_label(today_str):
            return

        day_label=self.create_top_day_time_label(today_str)
        item=QStandardItem()
        item.setSizeHint(day_label.sizeHint())
        self.chat_free_model.appendRow(item)
        index =self.chat_free_model.indexFromItem(item)
        if self.current_central_chat_frame=='first':
            self.chat_list_view_1.setIndexWidget(index,day_label)
        else:
            self.chat_list_view_2.setIndexWidget(index,day_label)

    def _get_last_rendered_day(self):
        if self.current_central_chat_frame=='first':
            list_view= self.chat_list_view_1
        else:
            list_view=self.chat_list_view_2
        for row in range(self.chat_free_model.rowCount()-1,-1,-1):
            item=self.chat_free_model.item(row)
            index=self.chat_free_model.indexFromItem(item)
            widget=list_view.indexWidget(index)
            t=getattr(widget,'_time',None)
            if t:
                return datetime.fromisoformat(t).astimezone().strftime("%Y-%m-%d")
        return None

    def one(self,temp_data_for_label):
        if self.temp_label_for_encryption is not None:
            self.temp_label_for_encryption.deleteLater()
            self.temp_label_for_encryption=None

        self.temp_label_for_encryption=self.create_text_msg_label(temp_data_for_label)
        x=20
        y=95
        self.temp_label_for_encryption.setParent(self.view_encrypt_white_bg_label)
        self.temp_label_for_encryption.move(x,y)
        self.temp_label_for_encryption.show()
        h=self.view_bottom_button_frame.y() + self.view_bottom_button_frame.height()
        self.view_encrypt_white_bg_label.setFixedSize(811,h)
        y=int((self.height()-h)//3.8)
        self.view_encrypt_content_frame.move(580,y)
        self.view_encrypt_content_frame.setFixedSize(840,h+30)

    def decrypt_and_view_msg(self,data):
        self.hide_and_clear_all_encrypt_all_stuff()
        self.view_encrypt_msg_main_frame.show()
        self.view_encrypt_msg_main_frame.move(0,0)
        self.view_encrypted_password_lineEdit.clear()
        self.view_encrypted_password_lineEdit.setFocus()
        temp_data_for_label =dict(data)
        temp_data_for_label['is_temp']=True
        temp_data_for_label['msg_content']=f"{data['msg_content'][:20]}..."
        temp_data_for_label['sent_by']=self.user_is_on_freind
        QTimer.singleShot(0,lambda:self.schedule_temp_label(temp_data_for_label))

        h=self.view_bottom_button_frame.y() + self.view_bottom_button_frame.height()
        self.view_encrypt_white_bg_label.setFixedSize(811,h)
        y=int((self.height()-h)//3.8)
        self.view_encrypt_content_frame.move(580,y)
    
    def push_a_single_locked_msg(self, data):
        data = self._view(data)
        self.ensure_day_label_for_today()
        label = self.create_locked_msg_label(data)
        if label is None:
            return
        self.push_locked_label_in_chat(label)

    def push_locked_label_in_chat(self,label):
        try:
            item=QStandardItem()
            size=label.sizeHint()
            size.setHeight(size.height()+10)
            item.setSizeHint(size)
            self.chat_free_model.appendRow(item)
            index=self.chat_free_model.indexFromItem(item)
            if self.current_central_chat_frame=='first':
                self.chat_list_view_1.setIndexWidget(index,label)
            else:
                self.chat_list_view_2.setIndexWidget(index,label)
            self.rebuild_day_labels()
        except Exception:
            print('error in push_locked_label_in_chat:\n'+traceback.format_exc())



    def delete_for_all_button_clicked(self):
        if self.user_msg_to_delete is None:
            return
        self.delete_main_frame.hide()
        self.push_a_single_delete_from_everyone_via_id(self.user_msg_to_delete)

    def delete_from_me_clicked(self):
        try:
            if self.user_msg_to_delete is None:
                return
            data=self.user_msg_to_delete
            target_index=self.check_for_left_side_msg_time_label_update(data)
            if target_index is not None:
                self.delete_label_by_index(target_index)

            self.remove_orphaned_day_labels()

            sent_by=data.get('sent_by',self.current_user_email)
            sent_to=data.get('sent_to') or (self.user_is_on_freind if sent_by == self.current_user_email else self.current_user_email)
            chat_id=get_local_chat_id(sent_by,sent_to)
            updated=dict(data)
            updated['delete_from_me']=True
            self.local_messages_db.upsert_message(updated,chat_id)

            t=DeleteForMeThread(data['_id'] ,self.current_user_email)
            t.error.connect(lambda e:print(f'delete_for_me failed: {e}'))
            self._track_thread(t)

            self.delete_frame_cancel_button_clicked()
            self.non_curr_delete_frame_cancel_button_clicked()
        except Exception:
            print('error in delete_from_me_clicked:\n'+traceback.format_exc())

    def delete_label_by_index(self,index):
        try:
            if self.current_central_chat_frame=='first':
                list_view=self.chat_list_view_1
            else:
                list_view =self.chat_list_view_2
            widget=list_view.indexWidget(index)
            if widget is not None:
                widget.deleteLater()
            row=index.row()
            self.chat_free_model.removeRow(row)
        except Exception:
            print('error in delete_label_by_index:\n'+traceback.format_exc())

    def delete_label_by_id(self,target_id):
        try:
            if self.current_central_chat_frame=='first':
                list_view = self.chat_list_view_1
            else:
                list_view=self.chat_list_view_2
            for row in range(self.chat_free_model.rowCount()):
                item=self.chat_free_model.item(row)
                index=self.chat_free_model.indexFromItem(item)
                widget=list_view.indexWidget(index)
                if widget is not None and getattr(widget,'_id',None)==target_id:
                    widget.deleteLater()
                    self.chat_free_model.removeRow(row)
                    return True


            return False
        except Exception:
            print('error in delete_label_by_id:\n'+traceback.format_exc())

    def check_for_left_side_msg_time_label_update(self,deleted_data_dict):
        try:
            if self.current_central_chat_frame=='first':
                list_view=self.chat_list_view_1
            else:
                list_view=self.chat_list_view_2

            target_index=None
            for row in range(self.chat_free_model.rowCount()-1,-1,-1):
                item=self.chat_free_model.item(row)
                index=self.chat_free_model.indexFromItem(item)
                widget=list_view.indexWidget(index)
                if widget is not None and getattr(widget,'_id',None)==deleted_data_dict['_id']:
                    target_index=index
                    break

            dict_target_email=deleted_data_dict['sent_to'] if deleted_data_dict['sent_by']==self.current_user_email else deleted_data_dict['sent_by']
            friend_entry=self.friend_list_left_label_dict.get(dict_target_email)

            if friend_entry is not None:
                msg_label=friend_entry["label"].m_label
                time_labl=friend_entry["label"].t_label

                prev_widget=None
                type_=content_=id_=time_ =sent_by_=None

                for row in range(self.chat_free_model.rowCount()-1,-1,-1):
                    if row==target_index.row():
                        continue


                    item=self.chat_free_model.item(row)
                    index=self.chat_free_model.indexFromItem(item)
                    candidate=list_view.indexWidget(index)
                    if candidate is None:
                        continue

                    c_type_=getattr(candidate,'_type',None)
                    c_content_=getattr(candidate,'_msg_content',None)
                    c_id_=getattr(candidate,'_id',None)
                    c_time_ = getattr(candidate,'_time',None)
                    c_sent_by_=getattr(candidate,'_sent_by',None)

                    if None in (c_type_,c_content_,c_id_,c_time_,c_sent_by_):
                        continue

                    prev_widget=candidate
                    type_,content_,id_,time_,sent_by_=c_type_,c_content_,c_id_,c_time_,c_sent_by_
                    break


                if prev_widget is None:
                    for i in msg_label.findChildren(QWidget,options=Qt.FindDirectChildrenOnly):
                        i.deleteLater()
                    self.set_text_to_stuff(msg_label,'')
                    self.set_text_to_stuff(time_labl,'')
                    friend_entry['last_msg_sent_at']=None
                    friend_entry['last_msg_id']=None
                else:
                    is_curr=self.current_user_email==sent_by_

                    self.set_text_to_stuff(time_labl,datetime.fromisoformat(time_).astimezone().strftime('%H:%M %p'))

                    if type_=='text':
                        for i in msg_label.findChildren(QWidget,options=Qt.FindDirectChildrenOnly):
                            i.deleteLater()


                        self.set_text_to_stuff(msg_label ,f'YOU : {content_}' if is_curr else content_)
                    elif type_=='sticker':
                        self.render_friend_preview_media(msg_label,is_curr ,'svg_icons/sticker.svg','STICKER')
                    elif type_=='gif':
                        self.render_friend_preview_media(msg_label,is_curr,'svg_icons/gif.svg','GIF')
                    elif type_=='image':
                        self.render_friend_preview_media(msg_label ,is_curr,'svg_icons/image.svg','Image')
                    elif type_=='encrypt':
                        self.render_friend_preview_media(msg_label,is_curr,'svg_icons/msg_lock.svg','Locked Message')
                    elif type_=='delete_from_everyone':
                        self.render_friend_preview_media(msg_label,is_curr,'svg_icons/delete.svg','Deleted Message')

                    friend_entry['last_msg_sent_at']=time_
                    friend_entry['last_msg_id']=id_

            return target_index

        except Exception:
            print('error in check_for_left_side_msg_time_label_update:\n'+traceback.format_exc())
            return None

    def render_friend_preview_media(self,msg_label,is_curr,icon_path,label_text):
        for i in msg_label.findChildren(QWidget,options=Qt.FindDirectChildrenOnly):
            i.deleteLater()
        msg_label.clear()
        if is_curr:
            x1__=38
            gap__=2
            s_o_s=14
            x2__=s_o_s + gap__ + x1__

            left_sticker_label=QLabel(msg_label)
            left_sticker_label.setFixedSize(s_o_s,s_o_s)
            left_sticker_label.move(x1__,3)
            left_sticker_label.raise_()
            left_sticker_label.show()

            left_sticker_text_label =QLabel(msg_label)
            left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
            left_sticker_text_label.setText(label_text)
            left_sticker_text_label.move(x2__,0)
            left_sticker_text_label.adjustSize()
            left_sticker_text_label.show()

            i_fall_a_sleep=QLabel(msg_label)
            i_fall_a_sleep.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px; font-weight:400;color:rgb(182,182,182); background:transparent;''')
            i_fall_a_sleep.setText('You : ')
            i_fall_a_sleep.move(3,0)
            i_fall_a_sleep.adjustSize()
            i_fall_a_sleep.show()

            self.set_svg_icon_and_color(left_sticker_label,icon_path,'#b6b6b6')

        else:
            msg_label.setText("")
            x1__=2
            gap__=2
            s_o_s=14
            x2__=s_o_s + gap__ + x1__

            left_sticker_label= QLabel(msg_label)
            left_sticker_label.setFixedSize(s_o_s,s_o_s)
            left_sticker_label.move(x1__,3)
            left_sticker_label.raise_()
            left_sticker_label.show()

            left_sticker_text_label=QLabel(msg_label)
            left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
            left_sticker_text_label.setText(label_text)
            left_sticker_text_label.move(x2__,0)
            left_sticker_text_label.adjustSize()
            left_sticker_text_label.show()

            self.set_svg_icon_and_color(left_sticker_label,icon_path,'#b6b6b6')

    def delete_frame_cancel_button_clicked(self):
        self.delete_main_frame.hide()
        self.user_msg_to_delete=None

    def non_curr_delete_frame_cancel_button_clicked(self):
        self.delete_main_frame.hide()
        self.user_msg_to_delete=None

    def on_sync_complete(self,remote_data:dict):
        for email,data in remote_data.items():
            self.store_last_seen_cache[email]={'is_online':bool(data.get('is_online')),'last_seen_at' : data.get('last_seen_at'),}


        if self.current_open_friend_email:
            cached=self.store_last_seen_cache.get(self.current_open_friend_email)
            if cached:
                self.on_friend_status_loaded({'is_online':cached.get('is_online',False),'last_seen_at':cached.get('last_seen_at'),})

    def start_last_seen_sync(self):
        if self.last_seen_sync_thread is not None and self.last_seen_sync_thread.isRunning():
            return
        self.last_seen_sync_thread=LastSeenSyncThread(current_user_email=self.current_user_email,friend_list=list(self.friend_list),local_db=self.local_last_seen_db,api_base_url= LiveActivityWebSocketThread.BASE_LIVE_ACTIVITY,local_cache_snapshot=dict(self.store_last_seen_cache),)
        self.last_seen_sync_thread.friend_status_changed.connect(self.on_friend_status_changed)
        self.last_seen_sync_thread.sync_complete.connect(self.on_sync_complete)
        self.last_seen_sync_thread.error.connect(lambda msg:print("last_seen sync error:",msg))
        self.last_seen_sync_thread.start()

    def on_ws_connected(self):
        self.ws_thread.request_sync(self.friend_list)
        # server forgets which chat we're on when the socket drops
        if self.current_open_friend_email:
            self.ws_thread.open_chat(self.current_open_friend_email)
        self.sync_after_reconnect()






    def on_friend_status_changed(self,email,is_online,last_seen_at):
        self.store_last_seen_cache[email]={'is_online':is_online,'last_seen_at':last_seen_at}
        if email==self.current_open_friend_email:
            self.on_friend_status_loaded({'is_online' : is_online,'last_seen_at':last_seen_at})

    def on_presence_update(self,data):
        email=data.get("email")
        if not email:
            return

        old=self.store_last_seen_cache.get(email)
        new={'is_online':data.get('is_online',False),'last_seen_at':data.get('last_seen_at') ,}

        if old==new:
            return

        self.store_last_seen_cache[email]=new

        db=self.local_last_seen_db
        threading.Thread(target=db.upsert,args= (email,new['last_seen_at'],new['is_online']),daemon=True).start()

        if email==self.current_open_friend_email:
            self.on_friend_status_loaded(data)

    def on_friend_status_loaded(self,data):
        email=self.current_open_friend_email
        if email:
            self.store_last_seen_cache[email]={'is_online':data.get('is_online',False),'last_seen_at':data.get('last_seen_at'),}

        if data.get('is_online' ,False):
            self.friend_online_label2.setStyleSheet(f'''background-color:{self.online_color}; border-radius:5px;border:2px solid white; ''')
            self.friend_online_label.setStyleSheet(f'''background-color:{self.online_color} ;border-radius:12px;border:3px solid white;''')
            self.friend_last_seen.setText('Online')

        else:
            self.friend_online_label2.setStyleSheet(f'''background-color:{self.offline_color};border-radius:5px;border:2px solid white;''')
            self.friend_online_label.setStyleSheet(f'''background-color:{self.offline_color};border-radius:12px;border:3px solid white;''')
            last_seen_at=data.get('last_seen_at')
            if last_seen_at:

                try:
                    self.set_last_seen(last_seen_at)
                except Exception:
                    self.friend_last_seen.setText(last_seen_at)

    def set_last_seen(self,str_date):

        dt=datetime.fromisoformat(str_date)

        dt= dt.replace(tzinfo=timezone.utc).astimezone()

        now=datetime.now().astimezone()

        if dt.date()==now.date():
            text=f"Last seen today at {dt.strftime('%I:%M:%S %p')}"
        elif dt.date()==(now-timedelta(days=1)).date():
            text= f"Last seen yesterday at {dt.strftime('%I:%M:%S %p')}"
        elif dt.year==now.year:
            text=f"Last seen on {dt.strftime('%d %b')} at {dt.strftime('%I:%M:%S %p')}"
        else:
            text=f"Last seen on {dt.strftime('%d %b %Y')} at {dt.strftime('%I:%M:%S %p')}"

        self.set_text_to_stuff(self.friend_last_seen,text)

    def on_mac_changed(self,registered_mac):
        pass

    def on_new_friend_added(self,new_friend_email):
        if new_friend_email not in self.friend_list:
            self.friend_list.append(new_friend_email)

    def friend_chat_button_clicked(self,email,data):
        try:
            global current_mac_id
            if self.current_central_chat_frame is None or self.current_central_chat_frame=='second':
                self.current_central_chat_frame='first'
                self.chat_list_view_2.hide()
                self.chat_list_view_1.show()
                self.clear_chat_model('second')
                self.chat_list_view_1.move(0,60)
                self.chat_list_view_1.setFixedSize(1101 , 861)
                self.free_chat_list_view=self.chat_list_view_1
                self.chat_free_model=self.chat_models['first']
                self.chat_list_view_1.setViewportMargins(0,0,0,0)
                self.chat_list_view_1.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
                self.chat_list_view_1.verticalScrollBar().setSingleStep(15)
            else:
                self.current_central_chat_frame='second'
                self.chat_list_view_2.show()
                self.chat_list_view_1.hide()
                self.free_chat_list_view=self.chat_list_view_2
                self.chat_free_model=self.chat_models['second']
                self.clear_chat_model('first')
                self.chat_list_view_2.setViewportMargins(0,0,0,0)
                self.chat_list_view_2.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
                self.chat_list_view_2.move(0,60)
                self.chat_list_view_2.setFixedSize(1101,861)
                self.chat_list_view_2.verticalScrollBar().setSingleStep(15)
            self.load_static_data_of_friend(email,data)
            self.current_open_friend_email=email
            self.ws_thread.open_chat(email)
            cached = self.store_last_seen_cache.get(email)
            if cached:
                self.on_friend_status_loaded({'is_online':cached.get('is_online',False),'last_seen_at':cached.get('last_seen_at')})
            self.touch_last_clicked(self.current_user_email,email)
            self.user_is_on_freind=email
            self.sendMeaasgePlainTextEdit.setFocus()
            self._stick_bottom=True
            self._history_exhausted=False
            self._set_loading(False)
            self.load_initial_chat(email)
        except Exception:
            print('error in friend_chat_button_clicked:\n'+traceback.format_exc())



    def filter_visible(self,msgs):
        out=[]
        for m in msgs:
            if m.get('sent_by')==self.current_user_email and int(m.get('delete_from_me')or 0):
                continue
            out.append(m)


        return out

    def load_initial_chat(self,friend_email):
        global CHAT_PAGE_SIZE
        chat_id=get_local_chat_id(self.current_user_email,friend_email)
        local_msgs=self.local_messages_db.get_latest(chat_id,limit=CHAT_PAGE_SIZE)
        self.populate_chat_from_messages(self.filter_visible(local_msgs))

        if len(local_msgs) < CHAT_PAGE_SIZE:
            self.fetch_older_from_server(friend_email,before_sent_at= None)

    def get_oldest_loaded_sent_at(self):
        if self.current_central_chat_frame=='first':
            list_view=self.chat_list_view_1
        else:
            list_view=self.chat_list_view_2
        for row in range(self.chat_free_model.rowCount()):
            item=self.chat_free_model.item(row)
            index = self.chat_free_model.indexFromItem(item)
            widget=list_view.indexWidget(index)
            t=getattr(widget,'_time',None)
            if t:
                return t
        return None

    def on_scrolled_to_top(self,friend_email):
        if self._loading_older or self._history_exhausted:
            return
        chat_id=get_local_chat_id(self.current_user_email,friend_email)
        oldest= self.get_oldest_loaded_sent_at()
        if oldest is None:
            return
        older_local=self.local_messages_db.get_before(chat_id , oldest,limit=CHAT_PAGE_SIZE)
        if older_local:
            self.prepend_messages_to_gui(self.filter_visible(older_local))
        else:
            self.fetch_older_from_server(friend_email,before_sent_at=oldest)

    def prepend_messages_to_gui(self,msgs,keep_position=True):
        existing=self.collect_currently_loaded_messages()
        anchor_id=existing[0]['_id'] if (existing and keep_position) else None
        self.populate_chat_from_messages(msgs+existing,anchor_id=anchor_id)



    def collect_currently_loaded_messages(self):
        if self.current_central_chat_frame=='first':
            list_view = self.chat_list_view_1
        else:
            list_view=self.chat_list_view_2
        out=[]
        for row in range(self.chat_free_model.rowCount()):
            item=self.chat_free_model.item(row)
            index=self.chat_free_model.indexFromItem(item)
            widget=list_view.indexWidget(index)
            _id=getattr(widget,'_id',None)
            if _id is None:
                continue
            full= self.local_messages_db.get_by_id(_id)
            if full is not None:
                out.append(full)
            else:
                out.append({'_id':_id,'msg_type':getattr(widget,'_type'),'msg_content':getattr(widget,'_msg_content'),'sent_by':getattr(widget,'_sent_by'),'sent_to':self.current_user_email if getattr(widget,'_sent_by')!=self.current_user_email else self.user_is_on_freind,'sent_at':getattr(widget,'_time'),'delete_from_me':False,'delete_from_all':False,})
        return out

    def fetch_older_from_server(self,friend_email,before_sent_at):
        self._set_loading(True)
        t=FetchOlderMessagesThread(self.current_user_email , friend_email,before_sent_at)
        t.finished_ok.connect(lambda msgs,fe=friend_email,b=before_sent_at:self.on_older_fetched(msgs,fe,b))
        t.error.connect(lambda e,fe=friend_email:self._on_older_error(e,fe))
        self._track_thread(t)

    def on_older_fetched(self, msgs, friend_email, before_sent_at=None):
        chat_id = get_local_chat_id(self.current_user_email, friend_email)
        for m in msgs:
            self.local_messages_db.upsert_message(m, chat_id)
            self.local_messages_db.mark_synced(m['_id'])

        if friend_email != self.current_open_friend_email:
            return
        self._set_loading(False)
        if len(msgs) < CHAT_PAGE_SIZE:
            self._history_exhausted = True
        if msgs:
            self.prepend_messages_to_gui(self.filter_visible(msgs), keep_position=before_sent_at is not None)

    def on_new_message_pushed(self, msg):
        if not msg:
            return
        msg = self._strip_local_fields(msg)
        chat_id = get_local_chat_id(msg['sent_by'], msg['sent_to'])
        self.local_messages_db.upsert_message(msg, chat_id)

        other = msg['sent_to'] if msg['sent_by'] == self.current_user_email else msg['sent_by']

        # always refresh the left friend-list row (and re-sort), whichever chat is open
        self.apply_last_message_preview(other, msg)

        if other != self.current_open_friend_email:
            return
        if msg['sent_by'] == self.current_user_email and int(msg.get('delete_from_me') or 0):
            return
        if self._widget_exists(msg.get('_id')):
            return

        msg_type = msg.get('msg_type')

        if msg_type == 'image':
            if msg.get('sent_by') == self.current_user_email:
                return
            self.resolve_and_show_image(msg)
        elif msg_type == 'sticker':
            self.push_a_single_sticker(msg)
        elif msg_type == 'gif':
            self.push_a_single_gif(msg)
        elif msg_type == 'encrypt':
            self.push_a_single_locked_msg(msg)
        elif msg_type == 'text':
            self.push_a_text_user_label(msg)
        else:
            print(f'unknown msg_type pushed: {msg_type}')




    def on_message_deleted_all_pushed(self, msg_id):
        try:
            full = self.local_messages_db.mark_deleted_for_all(msg_id)
            if full is None:
                return

            other = full['sent_to'] if full['sent_by'] == self.current_user_email else full['sent_by']
            entry = self.friend_list_left_label_dict.get(other)
            if entry is not None and entry.get('last_msg_id') == msg_id:
                # reset without triggering a reorder so the "don't clobber fresher" guard lets this through
                dict.__setitem__(entry, 'last_msg_sent_at', None)
                self.apply_last_message_preview(other, {**full, 'msg_type': 'delete_from_everyone'})

            self._replace_widget_with_deleted_label(full)
        except Exception:
            print('error in on_message_deleted_all_pushed:\n' + traceback.format_exc())
    def _replace_widget_with_deleted_label(self, data):
        model = getattr(self, 'chat_free_model', None)
        if model is None or self.current_central_chat_frame is None:
            return False
        list_view = self.chat_list_view_1 if self.current_central_chat_frame == 'first' else self.chat_list_view_2
    
        target_index = None
        for row in range(model.rowCount()):
            item = model.item(row)
            if item is None:
                continue
            index = model.indexFromItem(item)
            widget = list_view.indexWidget(index)
            if widget is not None and getattr(widget, '_id', None) == data['_id']:
                target_index = index
                break
    
        if target_index is None:
            return False
    
        data = self._view(data)
        data['msg_type'] = 'delete_from_everyone'
        data['delete_from_all'] = True
        data['is_temp'] = False
    
        new_label = self.create_delete_from_every_one_label(data)
        if new_label is None:
            return False
    
        old_widget = list_view.indexWidget(target_index)
        if old_widget is not None:
            old_widget.deleteLater()
    
        size = new_label.sizeHint()
        size.setHeight(size.height() + 10)
        self.chat_free_model.itemFromIndex(target_index).setSizeHint(size)
        list_view.setIndexWidget(target_index, new_label)
        return True


    
    def logout(self):
        try:
            self.ws_thread.close_chat()
            self.ws_thread.stop()
            self.internet_thread.stop()
            self.mac_watcher.stop()
            if self.last_seen_sync_thread:
                self.last_seen_sync_thread.quit()
            self.last_clicked_safety_net.stop()
        except Exception as e:
            print(f'error stopping threads on logout: {e}')

        self.local_messages_db.clear_all()

        import os as _os
        for f in ("messages.db","last_seen.db","last_clicked.db"):
            try:
                if _os.path.exists(f):
                    _os.remove(f)
            except Exception as e:
                print(f'could not remove {f}: {e}')


        self.close()

    def touch_last_clicked(self,my_email:str,target_email:str) -> str:
        chat_id=get_chat_id(my_email,target_email)
        my_field=sanitize_email(my_email)
        ts=datetime.now(timezone.utc).isoformat()

        blob=dict(self.last_clicked_cache.get(chat_id ,{}))
        blob[my_field]=ts
        self.last_clicked_cache[chat_id]=blob

        threading.Thread(target=self._touch_last_clicked_background,args=(chat_id,my_field,ts),daemon=True,).start()
        return ts

    def _touch_last_clicked_background(self,chat_id:str,field:str,ts:str) -> None:
        self.local_last_clicked_db.upsert_field(chat_id,field,ts)
        self.flush_last_clicked_pending()

    def flush_last_clicked_pending(self) -> None:
        if not self.ws_thread.is_connected():
            return

        pending = self.local_last_clicked_db.read_unsynced()
        if not pending:
            return

        entries=[{"chat_id":chat_id,"updates":data}for chat_id,data in pending]
        self._last_clicked_sent_snapshot={chat_id:data for chat_id,data in pending}
        self.ws_thread.push_last_clicked_batch(entries)

    def on_last_clicked_sync_ack(self , chat_ids:list) -> None:
        for chat_id in chat_ids:
            data=self._last_clicked_sent_snapshot.get(chat_id)
            if data is not None:
                self.local_last_clicked_db.mark_synced(chat_id,data)

    def on_offline(self):
        try:
            self.current_online_label.setStyleSheet(f'''background-color:{self.offline_color};border-radius:8px;border:3px solid white;''')

            for email in list(self.store_last_seen_cache):
                if self.store_last_seen_cache.get(email,{}).get('is_online'):
                    self.store_last_seen_cache[email]['is_online']=False
                    self.store_last_seen_cache[email]['last_seen_at']='Last known : Online'


            if not self.current_open_friend_email:
                return

            friend_data= self.store_last_seen_cache.get(self.current_open_friend_email)
            if friend_data and friend_data.get('last_seen_at')=='Last known : Online':
                self.friend_last_seen.setText('Last known : Online')
                self.friend_online_label2.setStyleSheet(f'''background-color:{self.offline_color};border-radius:5px;border:2px solid white;''')
                self.friend_online_label.setStyleSheet(f'''background-color:{self.offline_color};border-radius:12px;border:3px solid white;''')
        except Exception as e:
            print(f'error on_offline: {e}')

    def on_online(self, offline_seconds):
        self.current_online_label.setStyleSheet(
            f'''background-color:{self.online_color};border-radius:8px;border:3px solid white;''')
        self.start_last_seen_sync()
        self.flush_unsent_messages()

    def sticker_button_clicked(self):
        self.if_hide_all_3_on= True
        self.gif_frame.hide()
        self.sticker_frame.move(-10,550)
        self.sticker_frame.show()
        self.sticker_left_triangle_table.hide()
        self.sticker_down_triangle_table.show()

    def plus_sticker_button_clicked(self):
        self.if_hide_all_3_on=True
        self.gif_frame.hide()
        self.sticker_frame.move(210,550)
        self.sticker_frame.show()
        self.sticker_left_triangle_table.show()
        self.sticker_down_triangle_table.hide()

    def plus_button_clicked(self):
        self.hide_all_three_chat_area_floters_sticker_gif_plus_popup()
        self.plus_pop_up_buttons_frame.show()

    def gif_button_clicked(self):
        self.if_hide_all_3_on=True
        self.sticker_frame.hide()
        self.gif_frame.show()
        self.gif_left_triangle_table.hide()
        self.gif_frame.move(-10,550)
        self.gif_down_triangle_table.show()

    def gif_plus_button_clicked(self):
        self.if_hide_all_3_on=True
        self.sticker_frame.hide()
        self.gif_frame.show()
        self.gif_left_triangle_table.show()
        self.gif_frame.move(210,550)
        self.gif_down_triangle_table.hide()

    def hide_all_three_chat_area_floters_sticker_gif_plus_popup(self):
        self.gif_frame.hide()
        self.sticker_frame.hide()
        self.plus_pop_up_buttons_frame.hide()

    def on_mac_id_changed(self,server_mac_id,local_mac_id):
        print(f"Session hijacked / logged in elsewhere: server={server_mac_id} local={local_mac_id}")

    def on_other_last_clicked_changed(self,iso_ts):
        print(f"other user last clicked chat at {iso_ts}")

    def init_chat_theme_styleSheet(self):
        try:

            bottom_icon_button_icon_dict={self.plus_button:'svg_icons/plus.svg',self.image_button:'svg_icons/image.svg',self.gif_button:'svg_icons/gif.svg',self.sticker_button:'svg_icons/sticker.svg',self.microphone_button:'svg_icons/microphone.svg',self.password_msg_button:'svg_icons/password_msg_key.svg',self.send_button:'svg_icons/send.svg'}
            for i in bottom_icon_button_icon_dict:
                self.set_svg_icon_and_color(i,bottom_icon_button_icon_dict[i],self.current_chat_theme_styleSheet['bottom_icon_color'])
                i.setStyleSheet(self.current_chat_theme_styleSheet['bottom_button_stylesheet'])

            top_button_icon_dict={self.phone_button:'svg_icons/phone.svg',self.video_call_button:'svg_icons/video_call.svg'}
            for i in top_button_icon_dict:
                self.set_svg_icon_and_color(i,top_button_icon_dict[i] ,self.current_chat_theme_styleSheet['top_icon_color'])
                i.setStyleSheet(self.current_chat_theme_styleSheet['top_button_stylesheet'])

            self.bottom_input_frame.setStyleSheet(self.current_chat_theme_styleSheet['bottom_frame_stylesheet'])
            self.sendMeaasgePlainTextEdit.setStyleSheet(self.current_chat_theme_styleSheet['text_edit_stylesheet'])
            self.sendMeaasgePlainTextEdit.setPlaceholderText("Type a message...")
            self.friend_top_frame.setStyleSheet('background:transparent;')
            self.sendMeaasgePlainTextEdit.clear()
            self.a_label.setPixmap(QPixmap(self.current_chat_theme_styleSheet['chat_background']))
            self.set_rounded_corners(self.a_label)

            self.friend_name_label.setStyleSheet(self.current_chat_theme_styleSheet['name_stylesheet'])
            self.friend_last_seen.setStyleSheet(self.current_chat_theme_styleSheet['last_seen_stylesheet'])

        except Exception as e:
            traceback.print_exc()
            print(220,e)

    def start_loading_friend_list_label(self):

        self.friend_list_model.clear()

        for i in self.friend_list:
            if i in self.all_user_basic_info:
                user_data= self.all_user_basic_info[i]
                label=self.create_friend_list_label(user_data)
                self.push_friend_list_label(label)


    def push_a_single_friend_label(self,data):

        label=self.create_friend_list_label(data)
        self.push_friend_list_label(label)

    def push_friend_list_label(self ,label):
        if not self.chat_frame_default_image_label.isHidden():
            self.chat_frame_default_image_label.hide()
        item=QStandardItem()

        size = label.size()
        size.setHeight(size.height()+10)

        item.setSizeHint(size)

        self.friend_list_model.appendRow(item)

        index=self.friend_list_model.indexFromItem(item)
        self.friend_list_view.setIndexWidget(index,label)

    @staticmethod
    def _friend_sort_ts(iso):
        if not iso:
            return None
        try:
            return datetime.fromisoformat(iso).astimezone().timestamp()
        except Exception:
            return None

    def _schedule_friend_reorder(self):

        if getattr(self,'_friend_reorder_pending',False):
            return
        self._friend_reorder_pending=True
        QTimer.singleShot(0,self._reorder_friend_list)

    def _reorder_friend_list(self):
        self._friend_reorder_pending=False
        try:
            def sort_key(item):
                ts=self._friend_sort_ts(item[1].get('last_msg_sent_at'))
                return float('inf') if ts is None else -ts

            ordered=sorted(self.friend_list_left_label_dict.items(),key= sort_key)
            new_order=[email for email,_ in ordered]

            model=self.friend_list_model
            view=self.friend_list_view
            if model.rowCount()!=len(ordered):
                return
            if new_order==getattr(self,'_friend_current_order',None):
                return

            view.setUpdatesEnabled(False)
            try:
                for row in range(model.rowCount()):
                    index=model.index(row,0)
                    w=view.indexWidget(index)
                    if w is not None:
                        view.setIndexWidget(index,None)
                        QCoreApplication.removePostedEvents(w,QEvent.DeferredDelete)

                for row,(email,entry) in enumerate(ordered):
                    view.setIndexWidget(model.index(row,0),entry['label'])
            finally:
                view.setUpdatesEnabled(True)

            self._friend_current_order=new_order
        except Exception:
            print('error in _reorder_friend_list:\n'+traceback.format_exc())

    def closeEvent(self,event):
        self.ws_thread.close_chat()
        self.ws_thread.stop()
        self.internet_thread.stop()
        self.mac_watcher.stop()
        if self.chat_seen_thread:
            self.chat_seen_thread.stop()
        super().closeEvent(event)

    def load_static_data_of_friend(self,email,data):
        try:

            self.set_circular_image(self.friend_pfp_label,data['pfp_location'])
            self.set_circular_image(self.friend_pfp_label2,data['pfp_location'])

            self.friend_name_label.setText(data['name'])
            self.friend_name_label2.setText(data['name'])
            self.friend_mail_label.setText(data['email'])

            about_html=f'''<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0//EN" "http://www.w3.org/TR/REC-html40/strict.dtd"> <html><head><meta name="qrichtext" content="1" /><style type="text/css"> p,li {{ white-space:pre-wrap;}} </style></head><body style= " font-family:'MS Shell Dlg 2'; font-size:8.25pt; font-weight:400; font-style:normal;" bgcolor="transparent"> <p style=" margin-top:0px; margin-bottom:5px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px; line-height:110%;"><span style=" font-family:'Segoe UI,sans-serif'; font-size:14px; color:#4b5563;">{data['about']}</span></p> <p style=" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;"><span style=" font-family:'Segoe UI,sans-serif'; font-size:13px; color:#9ca3af;">Joined {datetime.fromisoformat(data.get('created_at', '1994-09-22T00:00:00')).strftime("%B %Y")}</span></p></body></html>'''
            self.about_textBrowser.setHtml(about_html)

            doc_height=self.about_textBrowser.document().size().height()
            self.about_textBrowser.move(11,10)
            self.about_textBrowser.setFixedHeight(int(doc_height)+5)
            self.about_frame.setGeometry(10,340,331,int(doc_height)+20)
            self.set_shadow_label_or_frame(self.about_frame,shadow='low',color_scheme='light')
        except Exception as e:
            print(f'error : load_static_data_of_friend : {e}')

    def changed(self):
        try:
            if self.if_hide_all_3_on:
                self.hide_all_three_chat_area_floters_sticker_gif_plus_popup()
                self.if_hide_all_3_on=False
            text=self.sendMeaasgePlainTextEdit.toPlainText()
            doc=self.sendMeaasgePlainTextEdit.document()

            newline_count=text.count("\n")
            block_count = doc.blockCount()

            line_count=max(block_count ,newline_count+1)

            max_lines=self.max_line_count

            line_height=self.sendMeaasgePlainTextEdit.fontMetrics().lineSpacing()

            base_edit_y=11
            base_frame_y=920

            base_edit_h=40
            base_frame_h=60

            grow_lines=min(line_count-1,max_lines)
            delta=grow_lines * line_height

            new_edit_h=base_edit_h + delta
            new_frame_h=base_frame_h + delta
            new_frame_y=base_frame_y - delta

            temp_edit_geometry=self.sendMeaasgePlainTextEdit.geometry()
            temp_frame_geometry=self.bottom_input_frame.geometry()

            self.sendMeaasgePlainTextEdit.setGeometry(temp_edit_geometry.x(),base_edit_y, temp_edit_geometry.width(),new_edit_h)

            self.bottom_input_frame.setGeometry(temp_frame_geometry.x() , new_frame_y,temp_frame_geometry.width(),new_frame_h)

            self.send_button.setGeometry(1050,11+delta,40,40)
            self.password_msg_button.setGeometry(1000,12+delta,36,36)

            all_4_button_frame=self.all_4_button_frame.geometry()
            self.all_4_button_frame.setGeometry(all_4_button_frame.x(),12+delta,all_4_button_frame.width(),all_4_button_frame.height())

            self.plus_frame.setGeometry(all_4_button_frame.x(),12+delta,all_4_button_frame.width(),all_4_button_frame.height())

            if line_count > max_lines:
                self.sendMeaasgePlainTextEdit.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            else:
                self.sendMeaasgePlainTextEdit.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

            if len(text) > 0 and self.expand_flag:
                self.contract_flag=True
                t=start_expanding()
                t.signal.connect(self.expand_verdict)
                self._track_thread(t)
                self.expand_flag=False

            elif len(text)==0 and self.contract_flag:
                self.expand_flag=True
                t=start_contracting()
                t.signal.connect(self.contract_verdict)
                self._track_thread(t)
                self.contract_flag=False

            new_frame_h=base_frame_y - delta - 60

            if self.current_central_chat_frame=='first':
                self.chat_list_view_1.setFixedSize(1100,new_frame_h)
            else:
                self.chat_list_view_2.setFixedSize(1100,new_frame_h)

            self._scroll_timer.start(0)
        except Exception as e:
            print(e)
            traceback.print_exc()

    def expand_verdict(self ,flag,x,w):
        geom=self.sendMeaasgePlainTextEdit.geometry()

        self.sendMeaasgePlainTextEdit.setGeometry(x,geom.y(),w,geom.height())
        self.all_4_button_frame.hide()
        self.plus_frame.show()

    def contract_verdict(self,flag,x,w):
        geom=self.sendMeaasgePlainTextEdit.geometry()

        self.sendMeaasgePlainTextEdit.setGeometry(x,geom.y(),w,geom.height())
        self.all_4_button_frame.show()
        self.plus_frame.hide()

    def get_refined_last_seen(self,iso_ts):
        if not iso_ts:
            return "offline"
        try:
            dt=datetime.fromisoformat(iso_ts)
            return dt.strftime("%b %d, %H:%M")
        except Exception:
            return "offline"

    def set_rounded_corners(self,label,radius=15):
        pixmap=label.pixmap()

        if pixmap is None or pixmap.isNull():
            return

        w=label.width()
        h=label.height()

        scaled=pixmap.scaled(w,h,Qt.KeepAspectRatioByExpanding,Qt.SmoothTransformation)

        rounded=QPixmap(w,h)
        rounded.fill(Qt.transparent)

        painter=QPainter(rounded)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setRenderHint(QPainter.SmoothPixmapTransform)

        path=QPainterPath()
        path.addRoundedRect(QRectF(0,0,w,h),radius,radius)

        painter.setClipPath(path)

        x=(scaled.width()-w) // 2
        y=(scaled.height()-h) // 2

        painter.drawPixmap(0,0,scaled , x,y,w,h)
        painter.end()

        label.setPixmap(rounded)

    def set_svg_icon_and_color(self,button,svg_path,color,icon_size=None,cache={}):
        is_label=isinstance(button,QLabel)

        if icon_size is None:
            if is_label:
                size=button.size()
            elif hasattr(button,"iconSize"):
                size=button.iconSize()
            else:
                size=button.size()
        elif isinstance(icon_size , int):
            size=QSize(icon_size,icon_size)
        else:
            size=icon_size

        if size.width()<=0 or size.height()<=0:
            print(f"[WARN] Invalid target size for {svg_path}, skipping icon set")
            return

        key=(svg_path,color,size.width(),size.height())
        if key in cache:
            cached=cache[key]
            if is_label:
                button.setPixmap(cached)
                button.setScaledContents(True)
            else:
                button.setIcon(cached)
                button.setIconSize(size)
            return

        with open(svg_path,"r",encoding="utf-8") as f:
            svg=f.read()

        def repl_fill(m):
            val=m.group(1)
            return m.group(0) if val.lower()=="none" else f'fill="{color}"'

        def repl_stroke(m):
            val=m.group(1)
            return m.group(0) if val.lower() =="none" else f'stroke="{color}"'

        svg=re.sub(r'fill=["\']([^"\']*)["\']',repl_fill,svg)
        svg=re.sub(r'stroke=["\']([^"\']*)["\']',repl_stroke,svg)
        svg = re.sub(r'(fill\s*:\s*)(?!none)[^;"\']+',rf'\1{color}',svg)
        svg=re.sub(r'(stroke\s*:\s*)(?!none)[^;"\']+',rf'\1{color}',svg)

        renderer=QSvgRenderer(QByteArray(svg.encode()))
        if not renderer.isValid():
            print(f"[WARN] Invalid SVG after recolor: {svg_path}")
            return

        dpr=button.devicePixelRatioF() if hasattr(button,"devicePixelRatioF") else 1.0
        pixmap=QPixmap(int(size.width()*dpr) ,int(size.height()*dpr))
        pixmap.setDevicePixelRatio(dpr)
        pixmap.fill(Qt.transparent)

        painter=QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setRenderHint(QPainter.SmoothPixmapTransform)
        renderer.render(painter,QRectF(0,0,size.width(),size.height()))
        painter.end()

        if is_label:
            cache[key]=pixmap
            button.setPixmap(pixmap)
            button.setScaledContents(True)
        else:
            icon=QIcon(pixmap)
            cache[key]=icon
            button.setIcon(icon)
            button.setIconSize(size)

    def set_text_to_stuff(self,widget,text):
        widget.ensurePolished()
        fm=QFontMetrics(widget.font())
        width=widget.width()

        elided= fm.elidedText(text,Qt.ElideRight,width)

        if isinstance(widget,QLineEdit):
            widget.setText(elided)

        elif isinstance(widget,QLabel):
            widget.setText(elided)

        elif isinstance(widget,(QTextEdit,QTextBrowser)):
            widget.setPlainText(elided)

        else:
            raise TypeError("Unsupported widget type")

    def set_circular_image(self,widget,path):
        QPixmapCache.clear()

        if isinstance(path,str) and path.lower().startswith(("http://","https://")):
            if not hasattr(self,'_nam'):
                self._nam=QNetworkAccessManager(self)

            req=QNetworkRequest(QUrl(path))
            reply=self._nam.get(req)
            reply.finished.connect(lambda:self._circular_image_downloaded(reply,widget))
            return

        pixmap=QPixmap()
        if not pixmap.load(path):
            pixmap.load("pfp/s_default.png")
        self._apply_circular_pixmap(widget,pixmap)

    def _circular_image_downloaded(self ,reply,widget):
        data=reply.readAll()
        pixmap=QPixmap()

        if reply.error() or not pixmap.loadFromData(data):
            pixmap.load("pfp/s_default.png")

        self._apply_circular_pixmap(widget,pixmap)
        reply.deleteLater()

    def _apply_circular_pixmap(self,widget,pixmap):
        size = min(widget.width(),widget.height())
        if size<=0:
            size=60

        pixmap=pixmap.scaled(size,size,Qt.KeepAspectRatioByExpanding,Qt.SmoothTransformation)

        x=max((pixmap.width()-size)//2,0)
        y=max((pixmap.height()-size)//2,0)
        pixmap=pixmap.copy(x,y,size ,size)

        result =QPixmap(size,size)
        result.fill(Qt.transparent)

        painter=QPainter(result)
        painter.setRenderHint(QPainter.Antialiasing,True)
        painter.setRenderHint(QPainter.SmoothPixmapTransform,True)

        path=QPainterPath()
        path.addEllipse(0,0,size,size)
        painter.setClipPath(path)
        painter.drawPixmap(0,0,pixmap)
        painter.end()

        if isinstance(widget,QLabel):
            widget.clear()
            widget.setPixmap(result)
            widget.setScaledContents(True)
            widget.update()
            widget.repaint()
            return

        if isinstance(widget,QPushButton):
            widget.setIcon(QIcon())
            widget.setFixedSize(size,size)
            widget.setStyleSheet(f"""QPushButton {{ border:none;background:transparent;border-radius:{size // 2}px;}}""")
            widget.setIcon(QIcon(result))
            widget.setIconSize(QSize(size,size))
            widget.update()
            widget.repaint()

    def set_shadow_label_or_frame(self, widget,blur_radius:int=30 ,x_offset:int=0,y_offset:int=6,color=(0,0,0),shadow:str="medium",enabled:bool=True,cache:bool=True,spread:float=0.0,glow:bool=False,color_scheme:str="auto") -> None:
        if widget is None:
            return

        if not enabled:
            widget.setGraphicsEffect(None)
            return

        opacity_map={"low":25,"medium":45,"high":75,"x-high" : 90}

        shadow_str =str(shadow).lower()
        if shadow_str in opacity_map:
            opacity=opacity_map[shadow_str]
        elif isinstance(shadow,(int,float)) and 0<=shadow<=100:
            opacity=int(shadow)
        else:
            opacity=45

        if color_scheme=="auto":
            if hasattr(widget,'palette'):
                bg=widget.palette().window().color()
                brightness=(bg.red()*299+bg.green()*587+bg.blue()*114) / 1000
                if brightness > 128:
                    opacity=min(100,opacity+15)
                else:
                    opacity=max(10,opacity-15)

        if cache:
            effect=widget.graphicsEffect()
            if isinstance(effect,QGraphicsDropShadowEffect):
                shadow_effect=effect
            else:
                shadow_effect=QGraphicsDropShadowEffect(widget)
        else:
            shadow_effect=QGraphicsDropShadowEffect(widget)

        blur=max(0,blur_radius)
        if spread > 0:
            blur=int(blur*(1-spread*0.5))
            y_offset=int(y_offset*(1+spread*0.3))
            x_offset=int(x_offset*(1+spread*0.3))

        shadow_effect.setBlurRadius(max(0,blur))
        shadow_effect.setOffset(x_offset,y_offset)

        if glow:
            shadow_effect.setBlurRadius(max(20,blur_radius*1.5))
            opacity=min(100,opacity+20)

            if isinstance(color,(tuple,list)):
                color=list(color)
                if len(color) == 3:
                    color=[min(255,c+30)for c in color] + [opacity]
                elif len(color)==4:
                    color=[min(255,color[i]+30)for i in range(3)] + [color[3]]
            elif isinstance(color ,QColor):
                c=QColor(color)
                c.setRed(min(255,c.red()+30))
                c.setGreen(min(255,c.green()+30))
                c.setBlue(min(255,c.blue()+30))
                color=c

        if isinstance(color,QColor):
            c=QColor(color)
            c.setAlpha(opacity if not isinstance(color,QColor)or color.alpha()==255 else color.alpha())
            shadow_effect.setColor(c)
        elif isinstance(color,(tuple ,list)):
            if len(color)==4:

                shadow_effect.setColor(QColor(*color))
            elif len(color)==3:

                shadow_effect.setColor(QColor(*color,opacity))
            else:
                shadow_effect.setColor(QColor(0,0,0,opacity))
        elif isinstance(color,str) and color.startswith('#'):
            hex_color=QColor(color)
            if hex_color.isValid():
                hex_color.setAlpha(opacity)
                shadow_effect.setColor(hex_color)
            else:
                shadow_effect.setColor(QColor(0,0,0,opacity))
        else:
            shadow_effect.setColor(QColor(0,0,0,opacity))

        widget.setGraphicsEffect(shadow_effect)

        if hasattr(widget,'setProperty'):
            widget.setProperty('shadow_metadata',{'blur':blur_radius,'opacity':opacity,'glow':glow,'spread':spread})

    def init_sticker(self):
        self.topStickerScrollAreaWidgetContents.setMaximumHeight(51)

        self.sticker_top_button_group=QButtonGroup(self)
        self.sticker_top_button_group.setExclusive(True)

        self.stick_top_button_list=[]
        self.sticker_button_folder={}

        import os
        p="sticker"
        c =0
        x_=10
        w=h=35
        gap=6

        for f in reversed(os.listdir(p)):
            folder_path = os.path.join(p, f)

            if os.path.isdir(folder_path):
                images = [
                    os.path.join(folder_path, i)
                    for i in os.listdir(folder_path)
                    if os.path.isfile(os.path.join(folder_path, i))
                ]

                images = [i.replace('\\', '/') for i in images]

                if len(images) > 0:
                    c += 1

                    button = QPushButton(self.topStickerScrollAreaWidgetContents)
                    button.setCheckable(True)
                    button.setGeometry(x_, 6, w, h)
                    button.setIcon(QIcon(images[0]))
                    button.setIconSize(QSize(25, 25))

                    button.setStyleSheet("""
                        QPushButton {
                            background: transparent;
                            border: none;
                            color: white;
                            padding: 8px 16px;
                        }
                        QPushButton:hover {
                            background: rgba(255,255,255,20);
                        }
                        QPushButton:checked {
                            border-bottom: 3px solid #3b82f6;
                            color: #3b82f6;
                        }
                        QPushButton:pressed {
                            background: rgba(255,255,255,30);
                        }
                    """)

                    button.setCursor(Qt.PointingHandCursor)
                    button.clicked.connect(
                        lambda checked, img=images: self.sticker_top_button_on_click(img)
                    )

                    self.sticker_top_button_group.addButton(button)
                    self.stick_top_button_list.append(button)
                    self.sticker_button_folder[button] = folder_path

                    x_ += w + gap
                    self.topStickerScrollAreaWidgetContents.setMinimumWidth(x_ + 5)

        self.stick_top_button_list[0].click()

    def sticker_top_button_on_click(self,image_list):
        self.start_populating_bottom_sticker_scroll_for_chat(image_list)

    def start_populating_bottom_sticker_scroll_for_chat(self, image_list):

        for i in self.bottomStickerScrollAreaWidgetContents.children():
            i.deleteLater()


        max_allowed=4
        width=75
        height=75
        gap_v= 8
        gap_h=15
        for i,j in enumerate(image_list):
            button=QPushButton(self.bottomStickerScrollAreaWidgetContents)
            x,y,w,h=self.get_sticker_cord(i,max_allowed,width,height,gap_v,gap_h)
            button.setGeometry(x ,y ,w,h)
            button.setIcon(QIcon(j))
            button.setIconSize(QSize(70,70))
            button.setCursor(Qt.PointingHandCursor)
            button.clicked.connect(lambda _,img=j:self.user_clicked_on_sticker_for_chat(img))
            button.show()

        self.bottomStickerScrollAreaWidgetContents.setMinimumHeight(((len(image_list)+max_allowed-1)//max_allowed)*(height+gap_v))


    
    
    def user_clicked_on_sticker_for_chat(self, name):
        try:
            self.sticker_frame.hide()
            self._stick_bottom = True
            ts = datetime.now(timezone.utc).isoformat()
            payload = {'_id': self.generate_random_id(), 'msg_type': 'sticker', 'msg_content': name, 'is_edited': False,
                       'sent_by': self.current_user_email, 'sent_at': ts, 'received_at': ts,
                       'sent_to': self.user_is_on_freind, 'delete_from_me': False, 'delete_from_all': False}
            self.push_a_single_sticker(payload)
            chat_id = get_local_chat_id(self.current_user_email, self.user_is_on_freind)
            self.local_messages_db.upsert_message(payload, chat_id)
            self._send_to_server(payload)
        except Exception:
            print('error in user_clicked_on_sticker_for_chat:\n' + traceback.format_exc())



    
    def get_sticker_cord(self,index,max_allowed=4,width=65,height=65,gap_v=3,gap_h=8):
        col=index % max_allowed
        row=index // max_allowed
        x=col * (width+gap_h) + 6
        y=row * (height+gap_v)
        return x,y , width,height

    def init_gif(self):
        self.topGifScrollAreaWidgetContents.setMaximumHeight(51)

        self.gif_top_button_group=QButtonGroup(self)
        self.gif_top_button_group.setExclusive(True)
        self.gif_top_button_list=[]
        self.gif_button_dict={}
        self.current_gif_labels_list=[]
        import os
        p="my_gif"
        c=0
        x_=10
        w=h=35
        gap=10

        for f in os.listdir(p):
            folder_path=os.path.join(p,f)
            if os.path.isdir(folder_path):
                gifs=[os.path.join(folder_path,i)for i in os.listdir(folder_path)if os.path.isfile(os.path.join(folder_path,i))]
                gifs=[i.replace('\\','/')for i in gifs]

                if len(gifs) > 0:
                    c+=1
                    gif_path=gifs[0]
                    button= QPushButton(self.topGifScrollAreaWidgetContents)
                    button.setCheckable(True)
                    button.setGeometry(x_,6,w,h)
                    button.setStyleSheet("""QPushButton { background:transparent;border:none;color:white;padding: 8px 16px;} QPushButton:hover { background:rgba(255,255,255,20);} QPushButton:checked { border-bottom:3px solid #3b82f6;color:#3b82f6;} QPushButton:pressed { background:rgba(255,255,255,30);}""")
                    button.setCursor(Qt.PointingHandCursor)

                    gif_to_e_played_label=QLabel(button)
                    gif_to_e_played_label.setGeometry(0,0,w-5,h-5)
                    gif_to_e_played_label.setScaledContents(True)

                    gif_movie=QMovie(gif_path)
                    gif_movie.setCacheMode(QMovie.CacheAll)

                    gif_to_e_played_label.setMovie(gif_movie)
                    gif_movie.start()

                    movies=getattr(self ,"movies",[])
                    movies.append(gif_movie)
                    x_+=w + gap
                    button.clicked.connect(lambda checked , gif_list=gifs:self.gif_top_button_on_click(gif_list))
                    self.gif_top_button_group.addButton(button)
                    self.topGifScrollAreaWidgetContents.setMinimumWidth(x_+5)
                    self.gif_top_button_list.append(button)
                    self.gif_button_dict[button]=folder_path

        self.gif_top_button_list[0].click()

    def gif_top_button_on_click(self,gif_list):
        self.start_populating_bottom_gif_scroll(gif_list)

    def start_populating_bottom_gif_scroll(self,gif_lists):
        self.gif_movie_label={}
        for i in self.bottomGifScrollAreaWidgetContents.children():
            i.deleteLater()


        self.current_gif_labels_list=[]
        self.gif_movie_label.clear()
        for i,j in enumerate(gif_lists):
            label=QLabel(self.bottomGifScrollAreaWidgetContents)
            x,y,w,h=self.get_gif_cord(i)
            label.setGeometry(x,y,w,h)
            label.setScaledContents(True)
            movie=QMovie(j)
            movie.setCacheMode(QMovie.CacheAll)
            label.setMovie(movie)
            label.gif_path=j
            movie.start()
            movie.stop()
            movie.jumpToFrame(0)
            label.installEventFilter(self)
            label.setCursor(Qt.PointingHandCursor)
            self.gif_movie_label[label]=movie
            self.current_gif_labels_list.append(label)
            label.show()

        max_allowed=3
        width=110
        height=110
        gap_v=13
        gap_h=10
        self.bottomGifScrollAreaWidgetContents.setMinimumHeight(((len(gif_lists)+max_allowed-1)//max_allowed)*(height+gap_v))

    def get_gif_cord(self,index,max_allowed=3,width=110,height=110,gap_v=10,gap_h=10):
        col= index % max_allowed
        row=index // max_allowed
        x=col * (width+gap_h) + 6
        y=row * (height+gap_v)
        return x,y,width ,height

    def eventFilter(self,obj,event):
        if obj==self.encrypt_text_plainTextEdit and event.type()==QEvent.KeyPress:
            if event.key() in (Qt.Key_Return,Qt.Key_Enter):
                if event.modifiers() & Qt.ShiftModifier:
                    return False

                self.check_for_ecryption_enter()
                return True

        if obj==self.encrypted_password_lineEdit and event.type()==QEvent.KeyPress:
            if event.key() in (Qt.Key_Return,Qt.Key_Enter):
                self.check_for_ecryption_enter()
                return True
        if obj in self.current_gif_labels_list:
            movie=self.gif_movie_label.get(obj)
            movie.start()
            if movie:
                if event.type()==QEvent.Enter:

                    pass

                elif event.type()==QEvent.MouseButtonPress:
                    self.gif_label_clicked(obj)
        if obj==self.sendMeaasgePlainTextEdit and event.type()==QEvent.KeyPress:
            if event.key() in (Qt.Key_Return,Qt.Key_Enter):
                if event.modifiers() & Qt.ShiftModifier:
                    return False

                self.send_button_clicked()
                return True

        if obj==self.delete_frame_bg_label:
            if event.type()==QEvent.MouseButtonPress:
                self.delete_main_frame.hide()
                return True

        if obj==self.view_encrypted_password_lineEdit and event.type()==QEvent.KeyPress:
            if event.key() in (Qt.Key_Return,Qt.Key_Enter):
                self.verify_decirptpassword_and_show_msg()
                return True

        if obj==self.view_encrypt_bg_label_main:
            if event.type()==QEvent.MouseButtonPress:
                self.hide_and_clear_all_encrypt_all_stuff()
                return True

        return super().eventFilter(obj,event)

    
    
    def gif_label_clicked(self, gif):
        try:
            self._stick_bottom = True
            ts = datetime.now(timezone.utc).isoformat()
            payload = {'_id': self.generate_random_id(), 'msg_type': 'gif', 'msg_content': gif.gif_path,
                       'is_edited': False, 'sent_by': self.current_user_email, 'sent_at': ts, 'received_at': ts,
                       'sent_to': self.user_is_on_freind, 'delete_from_me': False, 'delete_from_all': False}
            self.push_a_single_gif(payload)
            chat_id = get_local_chat_id(self.current_user_email, self.user_is_on_freind)
            self.local_messages_db.upsert_message(payload, chat_id)
            self._send_to_server(payload)
        except Exception:
            print('error in gif_label_clicked:\n' + traceback.format_exc())

    


    def generate_random_id(self,length=12):
        characters=string.ascii_letters + string.digits
        return ''.join(secrets.choice(characters)for _ in range(length))

    
    
    def send_button_clicked(self):
        try:
            msg = self.sendMeaasgePlainTextEdit.toPlainText().strip()
            if not msg:
                return
            self._stick_bottom = True
            ts = datetime.now(timezone.utc).isoformat()
            payload = {'_id': self.generate_random_id(), 'msg_type': 'text', 'msg_content': msg, 'is_edited': False,
                       'sent_by': self.current_user_email, 'sent_at': ts, 'received_at': ts,
                       'sent_to': self.user_is_on_freind, 'delete_from_me': False, 'delete_from_all': False}
            self.push_a_text_user_label(payload)
            chat_id = get_local_chat_id(self.current_user_email, self.user_is_on_freind)
            self.local_messages_db.upsert_message(payload, chat_id)
            self._send_to_server(payload)
            self.sendMeaasgePlainTextEdit.clear()
            self.changed()
        except Exception:
            print('error in send_button_clicked:\n' + traceback.format_exc())



    def normalize_blank_lines(self,text,max_consecutive_blank=1):

        lines=text.split('\n')
        out=[]
        blank_run=0
        for line in lines:
            if line.strip()=='':
                blank_run+=1
                if blank_run<=max_consecutive_blank:
                    out.append('')
            else:
                blank_run=0
                out.append(line)

        while out and out[0]== '':
            out.pop(0)
        while out and out[-1]=='':
            out.pop()

        return '\n'.join(out)

    def insert_soft_breaks(self,text,max_run=40):

        ZW='\u200b'
        out_lines=[]
        for line in text.split('\n'):
            chars=[]
            run_len=0
            for ch in line:
                if ch.isspace():
                    run_len =0
                    chars.append(ch)
                    continue

                if run_len!=0 and run_len % max_run==0:
                    chars.append(ZW)
                chars.append(ch)
                run_len+=1

            out_lines.append(''.join(chars))
        return '\n'.join(out_lines)

    def create_locked_msg_label(self,data):
        max_main_bg_w=1100
        extra_height_for_time=10
        main_w=340
        main_h=65

        transparent_long_bg_label=QLabel()
        transparent_long_bg_label.setStyleSheet('background:transparent;')
        is_curr=self.current_user_email==data.get('sent_by' , False)

        if is_curr:
            last_time=self.friend_list_left_label_dict[data['sent_to']]["last_msg_sent_at"]
            dict_target_email=data['sent_to']
        else:
            last_time= self.friend_list_left_label_dict[data['sent_by']]['last_msg_sent_at']
            dict_target_email=data['sent_by']

        if not data.get('is_temp',False):
            if last_time is None:
                msg_label=self.friend_list_left_label_dict[dict_target_email]["label"].m_label
                time_labl=self.friend_list_left_label_dict[dict_target_email]['label'].t_label
                self.set_text_to_stuff(time_labl,datetime.fromisoformat(data['sent_at']).astimezone().strftime('%H:%M %p'))
                msg_label.clear()

                for i in msg_label.findChildren(QWidget ,options=Qt.FindDirectChildrenOnly):
                    i.deleteLater()
                if is_curr:
                    x1__= 38
                    gap__=2
                    s_o_s= 14
                    x2__=s_o_s + gap__ + x1__
                    left_sticker_label=QLabel(msg_label)
                    left_sticker_label.setFixedSize(s_o_s,s_o_s)
                    left_sticker_label.move(x1__,3)
                    left_sticker_label.raise_()
                    left_sticker_label.show()

                    left_sticker_text_label= QLabel(msg_label)
                    left_sticker_text_label.setStyleSheet('''font-family: "Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                    left_sticker_text_label.setText('Locked Message')
                    left_sticker_text_label.move(x2__,0)
                    left_sticker_text_label.adjustSize()
                    left_sticker_text_label.show()

                    i_fall_a_sleep=QLabel(msg_label)
                    i_fall_a_sleep.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                    i_fall_a_sleep.setText('You : ')
                    i_fall_a_sleep.move(3,0)
                    i_fall_a_sleep.adjustSize()
                    i_fall_a_sleep.show()

                    self.set_svg_icon_and_color(left_sticker_label,'svg_icons/msg_lock.svg','#b6b6b6')

                else:
                    msg_label.setText("")
                    x1__=2
                    gap__=2
                    s_o_s=14
                    x2__=s_o_s + gap__ + x1__
                    left_sticker_label=QLabel(msg_label)
                    left_sticker_label.setFixedSize(s_o_s,s_o_s)
                    left_sticker_label.move(x1__,3)
                    left_sticker_label.raise_()
                    left_sticker_label.show()
                    left_sticker_text_label=QLabel(msg_label)
                    left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px ; font-weight:400;color:rgb(182,182,182);background:transparent;''')
                    left_sticker_text_label.setText('Locked Message')
                    left_sticker_text_label.move(x2__,0)
                    left_sticker_text_label.adjustSize()
                    left_sticker_text_label.show()
                    self.set_svg_icon_and_color(left_sticker_label,'svg_icons/msg_lock.svg','#b6b6b6')

                self.friend_list_left_label_dict[dict_target_email]['last_msg_sent_at'] =data['sent_at']
                self.friend_list_left_label_dict[dict_target_email]['last_msg_id']=data['_id']


            else:
                if last_time < data['sent_at']:
                    msg_label=self.friend_list_left_label_dict[dict_target_email]["label"].m_label
                    time_label=self.friend_list_left_label_dict[dict_target_email]['label'].t_label
                    self.set_text_to_stuff(time_label,datetime.fromisoformat(data['sent_at']).astimezone().strftime('%H:%M %p'))
                    msg_label.clear()

                    for i in msg_label.findChildren(QWidget,options=Qt.FindDirectChildrenOnly):
                        i.deleteLater()

                    if is_curr:
                        x1__=38
                        gap__= 10
                        s_o_s=14
                        x2__=s_o_s + gap__ + x1__
                        left_sticker_label=QLabel(msg_label)
                        left_sticker_label.setFixedSize(s_o_s,s_o_s)
                        left_sticker_label.move(x1__,3)
                        left_sticker_label.raise_()
                        left_sticker_label.show()

                        left_sticker_text_label=QLabel(msg_label)
                        left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size :14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                        left_sticker_text_label.setText('Locked Message')
                        left_sticker_text_label.move(x2__,0)
                        left_sticker_text_label.adjustSize()
                        left_sticker_text_label.show()

                        i_fall_a_sleep=QLabel(msg_label)
                        i_fall_a_sleep.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                        i_fall_a_sleep.setText('You : ')
                        i_fall_a_sleep.move(3,0)
                        i_fall_a_sleep.adjustSize()
                        i_fall_a_sleep.show()
                        self.set_svg_icon_and_color(left_sticker_label,'svg_icons/msg_lock.svg','#b6b6b6')

                    else:
                        msg_label.setText("")
                        x1__=2
                        gap__=2
                        s_o_s= 14
                        x2__=s_o_s + gap__ + x1__
                        left_sticker_label=QLabel(msg_label)
                        left_sticker_label.setFixedSize(s_o_s,s_o_s)
                        left_sticker_label.move(x1__,3)
                        left_sticker_label.raise_()
                        left_sticker_label.show()
                        left_sticker_text_label=QLabel(msg_label)
                        left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                        left_sticker_text_label.setText('Locked Message')
                        left_sticker_text_label.move(x2__,0)
                        left_sticker_text_label.adjustSize()
                        left_sticker_text_label.show()

                        self.set_svg_icon_and_color(left_sticker_label,'svg_icons/msg_lock.svg','#b6b6b6')
                    self.friend_list_left_label_dict[dict_target_email]['last_msg_sent_at']=data['sent_at']
                    self.friend_list_left_label_dict[dict_target_email]['last_msg_id']=data['_id']

        theme=self.current_chat_theme_styleSheet
        transparent_long_bg_label.setFixedSize(max_main_bg_w , 1)

        main_label=QLabel()
        main_label.setStyleSheet(self.current_chat_theme_styleSheet['encrypt_sender_bubble']if is_curr else self.current_chat_theme_styleSheet['encrypt_receiver_bubble'])
        main_label.setFixedSize(main_w,main_h)

        lock_icon=QLabel(main_label)
        lock_icon.setGeometry(0,16-10,52,39)
        lock_icon.setScaledContents(True)
        self.set_svg_icon_and_color(lock_icon,'svg_icons/lock.svg',theme['encrypt_sender_icon_color']if is_curr else theme['encrypt_receiver_icon_color'])

        head_label=QLabel(main_label)
        head_label.setGeometry(30,7-int(10*1.2),241 ,31+10)
        head_label.setStyleSheet(theme['encrypt_sender_head']if is_curr else theme['encrypt_receiver_head'])
        head_label.setText('Locked Message')

        bottom=QLabel(main_label)
        bottom.setGeometry(30,37-15,280,25+10)
        bottom.setStyleSheet(theme['encrypt_sender_bottom']if is_curr else theme['encrypt_receiver_bottom'])
        bottom.setText('This message is protected by a password')

        head_label.setAlignment(Qt.AlignTop|Qt.AlignLeft)
        bottom.setAlignment(Qt.AlignTop|Qt.AlignLeft)
        time_label=QLabel(main_label)
        time_label.setStyleSheet(theme['right_text_time_stylesheet']if is_curr else theme['left_text_time_stylesheet'])
        time_label.setFixedSize(41,15)
        alpha=15
        time_x = main_w - time_label.width() - extra_height_for_time
        time_y=int(main_h-alpha*1.4)
        time_label.move(time_x,time_y)
        time_label.setText(datetime.fromisoformat(data["sent_at"]).astimezone().strftime('%H:%M %p'))
        if not data.get('is_temp',False):
            main_label.setParent(transparent_long_bg_label)
            if is_curr:
                main_x=max_main_bg_w - main_label.width() - 20

            else:
                main_x=10
            main_label.move(main_x,0)
            transparent_long_bg_label.setFixedSize(max_main_bg_w,main_label.height())

            button_size=30
            button_gap=3
            y_of_button=int((transparent_long_bg_label.height()-button_size)//2)

            if is_curr:
                button_x=main_x - int(button_size*4.5+button_gap*3)

                report_x=button_x
                delete_x=report_x + button_size + button_gap
                edit_x=delete_x + button_size + button_gap
                copy_x=edit_x + button_size + button_gap

            else:
                button_x=main_label.width() + int(button_size//2+button_gap)

                copy_x=button_x
                edit_x=copy_x + button_size + button_gap
                delete_x=edit_x + button_size + button_gap
                report_x=delete_x + button_size + button_gap
            report_button=QPushButton(transparent_long_bg_label)
            report_button.setGeometry(report_x,y_of_button,button_size,button_size)
            report_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(report_button, 'svg_icons/chat_report.svg',self.current_chat_theme_styleSheet["chat_buttons_color"])

            report_button.setIconSize(QSize(int(button_size*0.5),int(button_size*0.5)))
            report_button.setCursor(Qt.PointingHandCursor)

            copy_button=QPushButton(transparent_long_bg_label)
            copy_button.setGeometry(copy_x,y_of_button,button_size,button_size)
            copy_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(copy_button ,'svg_icons/chat_copy.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.6))
            copy_button.setIconSize(QSize(int(button_size*0.6),int(button_size*0.6)))
            copy_button.setCursor(Qt.PointingHandCursor)

            delete_button=QPushButton(transparent_long_bg_label)
            delete_button.setGeometry(delete_x,y_of_button,button_size,button_size)
            delete_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(delete_button,'svg_icons/chat_bin2.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.7))
            delete_button.setIconSize(QSize(int(button_size*0.7),int(button_size*0.7)))
            delete_button.setCursor(Qt.PointingHandCursor)

            edit_button = QPushButton(transparent_long_bg_label)
            edit_button.setGeometry(edit_x,y_of_button,button_size,button_size)
            edit_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(edit_button,'svg_icons/edit.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.6))
            edit_button.setIconSize(QSize(int(button_size*0.7),int(button_size*0.7)))
            edit_button.setCursor(Qt.PointingHandCursor)

            report_button.hide()
            copy_button.hide()
            delete_button.hide()
            edit_button.hide()

            def show_inside_buttons(event):
                report_button.show()
                copy_button.show()
                delete_button.show()
                edit_button.show()

            def hide_inside_buttons(event):
                report_button.hide()
                copy_button.hide()
                delete_button.hide()
                edit_button.hide()

            transparent_long_bg_label.enterEvent=show_inside_buttons
            transparent_long_bg_label.leaveEvent=hide_inside_buttons
            transparent_long_bg_label._id=data['_id']
            transparent_long_bg_label._type=data['msg_type']
            transparent_long_bg_label._msg_content=data['msg_content']
            transparent_long_bg_label._time=data['sent_at']
            transparent_long_bg_label._sent_by=data['sent_by']

            delete_button.clicked.connect(lambda:self.delete_a_chat_widget(data))

            def main_label_clicked(event):
                if event.button()==Qt.LeftButton:
                    self.decrypt_and_view_msg(data)
                    self.password_protected_data_dict=data

            def enter_event(event):
                main_label.setCursor(Qt.PointingHandCursor)

            def leave_event(event):
                main_label.setCursor(Qt.ArrowCursor)

            main_label.enterEvent=enter_event
            main_label.leaveEvent=leave_event
            main_label.mousePressEvent=main_label_clicked

            return transparent_long_bg_label

        return main_label

    def create_text_msg_label(self,data):
        try:

            max_main_bg_w=1100

            display_max_width=750
            display_max_height=400
            min_width=50
            extra_height_for_time=10

            transparent_long_bg_label=QLabel()
            transparent_long_bg_label.setStyleSheet('background:transparent;')
            is_curr=self.current_user_email==data.get('sent_by',False)

            if is_curr:
                last_time=self.friend_list_left_label_dict[data['sent_to']]["last_msg_sent_at"]
                dict_target_email=data['sent_to']
            else:
                last_time=self.friend_list_left_label_dict[data['sent_by']]['last_msg_sent_at']
                dict_target_email=data['sent_by']

            if not data.get('is_temp',False):
                if last_time is None:
                    msg_label=self.friend_list_left_label_dict[dict_target_email]["label"].m_label
                    time_labl=self.friend_list_left_label_dict[dict_target_email]['label'].t_label
                    for i in msg_label.findChildren(QWidget,options=Qt.FindDirectChildrenOnly):
                        i.deleteLater()

                    self.set_text_to_stuff(time_labl,datetime.fromisoformat(data['sent_at']).astimezone().strftime('%H:%M %p'))
                    self.set_text_to_stuff(msg_label,f'YOU : {data["msg_content"]}' if is_curr else data["msg_content"])
                    self.friend_list_left_label_dict[dict_target_email]['last_msg_sent_at']=data['sent_at']
                    self.friend_list_left_label_dict[dict_target_email]['last_msg_id']=data['_id']

                else:
                    if last_time < data['sent_at']:
                        msg=data["msg_content"].replace("\n"," ")
                        msg_label=self.friend_list_left_label_dict[dict_target_email]["label"].m_label
                        for i in msg_label.findChildren(QWidget,options=Qt.FindDirectChildrenOnly):
                            i.deleteLater()

                        time_label=self.friend_list_left_label_dict[dict_target_email]['label'].t_label
                        self.set_text_to_stuff(time_label,datetime.fromisoformat(data['sent_at']).astimezone().strftime('%H:%M %p'))
                        self.set_text_to_stuff(msg_label,f'YOU : {msg}' if is_curr else msg)
                        self.friend_list_left_label_dict[dict_target_email]['last_msg_sent_at']=data['sent_at']
                        self.friend_list_left_label_dict[dict_target_email]['last_msg_id']=data['_id']




            theme=self.current_chat_theme_styleSheet
            transparent_long_bg_label.setFixedSize(max_main_bg_w,1)

            time_label=QLabel()
            time_label.setStyleSheet(theme['right_text_time_stylesheet']if is_curr else theme['left_text_time_stylesheet'])
            time_label.setFixedSize(41,15)

            bubble_stylesheet=theme['current_user_stylesheet'] if is_curr else theme['receiver_user_stylesheet']

            provied_text=data.get('msg_content','N/A')
            provied_text=self.normalize_blank_lines(provied_text)
            provied_text =self.insert_soft_breaks(provied_text)

            temp_label_for_size=QLabel()
            temp_label_for_size.setStyleSheet(bubble_stylesheet)
            temp_label_for_size.setText(provied_text)
            temp_label_for_size.setWordWrap(True)
            temp_label_for_size.ensurePolished()

            fm=temp_label_for_size.fontMetrics()

            margins_=temp_label_for_size.contentsMargins()
            extra_w=margins_.left() + margins_.right()
            extra_h=margins_.top() + margins_.bottom()

            rect = fm.boundingRect(0,0,10000,10000,Qt.TextWordWrap|Qt.AlignLeft|Qt.AlignTop,provied_text)

            width=rect.width() + extra_w
            height=rect.height() + extra_h * 2

            temp_label_for_size.setFixedSize(int(width),int(height))

            if width < min_width:
                width=min_width

            if width > display_max_width or height > display_max_height:
                alpha=15
                main_content_bg=QLabel()
                main_content_bg.setStyleSheet(bubble_stylesheet)
                main_content_bg.setFixedSize(display_max_width+10 if width>display_max_width else width+10 ,display_max_height+alpha if height>display_max_height else height+alpha)

                time_label.setParent(main_content_bg)
                time_x=main_content_bg.width() - time_label.width() - margins_.left()
                time_y=int(main_content_bg.height()-alpha*1.4)
                time_label.move(time_x,time_y)
                tb=QTextBrowser(main_content_bg)
                tb.move(int(margins_.left()//2),0)
                tb.setFixedSize(main_content_bg.width()-alpha,display_max_height if height>display_max_height else height)
                tb.setStyleSheet("""QScrollBar:vertical { background:transparent;width:8px;margin:0px;border:none;} QScrollBar::handle:vertical { background:rgba(255, 255, 255, 60);border-radius:4px;min-height:24px;} QScrollBar::handle:vertical:hover { background: rgba(255, 255, 255, 100);} QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height:0px;border:none;background:transparent;} QScrollBar::add-page : vertical, QScrollBar::sub-page:vertical { background:transparent;} QScrollBar:horizontal { background:transparent;height:8px;margin:0px;border : none;} QScrollBar::handle:horizontal { background:rgba(255, 255, 255, 60);border-radius:4px;min-width:24px;} QScrollBar::handle:horizontal:hover { background:rgba(255, 255, 255, 100);} QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width: 0px;border:none;background:transparent;} QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal { background:transparent;}""")
                tb.setText(provied_text)
                tb.setContentsMargins(0,0,0,0)
                tb.setViewportMargins(0,0,0 ,0)
                tb.document().setDocumentMargin(0)

                tb.setWordWrapMode(QTextOption.WrapAnywhere)
                time_label.raise_()


            else:
                main_content_bg=QLabel()
                main_content_bg.setStyleSheet(bubble_stylesheet)
                main_content_bg.setAlignment(Qt.AlignTop)
                main_content_bg.setFixedSize(width+margins_.left()+10,int(height+extra_height_for_time//2))
                main_content_bg.setText(provied_text)
                time_label.setParent(main_content_bg)
                time_x=int(main_content_bg.width()-time_label.width()-margins_.left())
                time_y=int(main_content_bg.height()-extra_height_for_time*1.8)
                time_label.move(time_x,time_y)
            local_time=datetime.fromisoformat(data["sent_at"]).astimezone()
            time_label.setText(f'{local_time.strftime("%I:%M %p")}')

            if is_curr:
                main_x=max_main_bg_w - main_content_bg.width() - 20
            else:
                main_x=10

            if not data.get('is_temp',False):
                main_content_bg.setParent(transparent_long_bg_label)
                main_content_bg.move(main_x,0)
                transparent_long_bg_label.setFixedSize(max_main_bg_w,main_content_bg.height())

                button_size=30
                button_gap= 3
                y_of_button=int((transparent_long_bg_label.height()-button_size)//2)

                if is_curr:
                    button_x= main_x - int(button_size*4.5+button_gap*3)

                    report_x= button_x
                    delete_x=report_x + button_size + button_gap
                    edit_x=delete_x + button_size + button_gap
                    copy_x=edit_x + button_size + button_gap

                else:
                    button_x= main_content_bg.width() + int(button_size//2+button_gap)

                    copy_x=button_x
                    edit_x=copy_x + button_size + button_gap
                    delete_x=edit_x + button_size + button_gap
                    report_x=delete_x + button_size + button_gap
                report_button=QPushButton(transparent_long_bg_label)
                report_button.setGeometry(report_x,y_of_button,button_size,button_size)
                report_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
                self.set_svg_icon_and_color(report_button,'svg_icons/chat_report.svg',self.current_chat_theme_styleSheet["chat_buttons_color"])

                report_button.setIconSize(QSize(int(button_size*0.5),int(button_size*0.5)))
                report_button.setCursor(Qt.PointingHandCursor)

                copy_button=QPushButton(transparent_long_bg_label)
                copy_button.setGeometry(copy_x,y_of_button,button_size,button_size)
                copy_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
                self.set_svg_icon_and_color(copy_button,'svg_icons/chat_copy.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.6))
                copy_button.setIconSize(QSize(int(button_size*0.6),int(button_size*0.6)))
                copy_button.setCursor(Qt.PointingHandCursor)

                delete_button=QPushButton(transparent_long_bg_label)
                delete_button.setGeometry(delete_x,y_of_button,button_size,button_size)
                delete_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
                self.set_svg_icon_and_color(delete_button,'svg_icons/chat_bin2.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.7))
                delete_button.setIconSize(QSize(int(button_size*0.7),int(button_size*0.7)))
                delete_button.setCursor(Qt.PointingHandCursor)

                edit_button=QPushButton(transparent_long_bg_label)
                edit_button.setGeometry(edit_x,y_of_button,button_size,button_size)
                edit_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
                self.set_svg_icon_and_color(edit_button,'svg_icons/edit.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.6))
                edit_button.setIconSize(QSize(int(button_size*0.7),int(button_size*0.7)))
                edit_button.setCursor(Qt.PointingHandCursor)

                report_button.hide()
                copy_button.hide()
                delete_button.hide()
                edit_button.hide()

                def show_inside_buttons(event):
                    report_button.show()
                    copy_button.show()
                    delete_button.show()
                    edit_button.show()

                def hide_inside_buttons(event):
                    report_button.hide()
                    copy_button.hide()
                    delete_button.hide()
                    edit_button.hide()

                transparent_long_bg_label.enterEvent=show_inside_buttons
                transparent_long_bg_label.leaveEvent =hide_inside_buttons
                transparent_long_bg_label._id=data['_id']
                transparent_long_bg_label._type=data['msg_type']
                transparent_long_bg_label._msg_content=data['msg_content']
                transparent_long_bg_label._time=data['sent_at']
                transparent_long_bg_label._sent_by=data['sent_by']

                delete_button.clicked.connect(lambda:self.delete_a_chat_widget(data))

            if data.get('is_temp'):
                return main_content_bg

            return transparent_long_bg_label
        except Exception as e:
            print(f'error in create_text_msg_label : {e}')
            traceback.print_exc()
            return None

    def delete_a_chat_widget(self,data):
        if self.temp_label_for_deleting is not None:
            self.temp_label_for_deleting.deleteLater()
            self.temp_label_for_deleting=None

        self.delete_main_frame.show()
        self.delete_main_frame.move(0,0)
        data['is_temp']=True
        if data.get('msg_type')=='text':
            temp_label=self.create_text_msg_label(data)
        elif data.get('msg_type')=='sticker':
            temp_label= self.create_sticker_msg_label(data)
        elif data.get('msg_type')=='gif':
            temp_label=self.create_gif_msg_label(data)
        elif data.get('msg_type')=='encrypt':
            temp_label=self.create_locked_msg_label(data)
        elif data.get('msg_type')=='image':
            temp_label=self.create_image_msg_label(data)
        elif data.get('msg_type') =='delete_from_everyone':
            temp_label=self.create_delete_from_every_one_label(data)

        temp_label.setParent(self.delete_content_frame)
        temp_label.move(40 ,110)
        temp_label.show()
        content_starting_y=110
        frame_y=content_starting_y + temp_label.height() + 20
        self.temp_label_for_deleting=temp_label
        if data['sent_by']==self.current_user_email:
            self.delete_button_frame.move(33,frame_y)
            selected_=self.delete_button_frame
            self.non_curr_delete_button_frame.hide()
            self.delete_button_frame.show()

        else:
            self.non_curr_delete_button_frame.move(33,frame_y)
            selected_=self.non_curr_delete_button_frame
            self.delete_button_frame.hide()
            self.non_curr_delete_button_frame.show()

        height_of_white_bg=selected_.height() + +selected_.y()
        self.delete_white_bg_label.setFixedSize(self.delete_white_bg_label.width() , height_of_white_bg)

        y=int((self.height()-height_of_white_bg)//2.5)
        self.delete_content_frame.move(540,y)

        self.user_msg_to_delete=data
    
    def push_a_text_user_label(self, data):
        data = self._view(data)
        self.ensure_day_label_for_today()
        label = self.create_text_msg_label(data)
        if label is None:
            return
        self.push_text_label_in_chat(label)


    
    def push_text_label_in_chat(self,label):
        item=QStandardItem()

        size=label.sizeHint()
        size.setHeight(size.height()+10)

        item.setSizeHint(size)

        self.chat_free_model.appendRow(item)

        index=self.chat_free_model.indexFromItem(item)
        if self.current_central_chat_frame=='first':
            self.chat_list_view_1.setIndexWidget(index,label)
        else:
            self.chat_list_view_2.setIndexWidget(index,label)
        self.rebuild_day_labels()

    def push_a_single_sticker(self,data):
        self.ensure_day_label_for_today()

        label=self.create_sticker_msg_label(data)
        if label is None:
            return
        self.push_sticker_label_in_chat(label)

    def create_sticker_msg_label(self,data):

        max_main_bg_w=1100
        sticker_width=180
        sticker_height=180
        extra_height_for_time=20

        transparent_long_bg_label=QLabel()
        transparent_long_bg_label.setStyleSheet('background:transparent;')
        is_curr=self.current_user_email==data.get('sent_by',False)

        if is_curr:
            last_time = self.friend_list_left_label_dict[data['sent_to']]["last_msg_sent_at"]
            dict_target_email=data['sent_to']
        else:
            last_time=self.friend_list_left_label_dict[data['sent_by']]['last_msg_sent_at']
            dict_target_email=data['sent_by']

        theme =self.current_chat_theme_styleSheet

        sticker_label=QLabel()
        sticker_label.setFixedSize(sticker_width,sticker_height)
        sticker_label.setStyleSheet("background: transparent;")

        sticker_image_label=QLabel(sticker_label)
        sticker_image_label.setGeometry(0,0,160,160)
        sticker_image_label.setScaledContents(True)
        sticker_image_label.setPixmap(QPixmap(data["msg_content"]))
        sticker_image_label.show()
        if is_curr:
            main_x=max_main_bg_w - sticker_label.width() - 20

        else:
            main_x=10

        if not data.get('is_temp',False):
            if last_time is None:
                msg_label=self.friend_list_left_label_dict[dict_target_email]["label"].m_label
                time_labl=self.friend_list_left_label_dict[dict_target_email]['label'].t_label
                self.set_text_to_stuff(time_labl,datetime.fromisoformat(data['sent_at']).astimezone().strftime('%H:%M %p'))
                msg_label.clear()

                for i in msg_label.findChildren(QWidget,options=Qt.FindDirectChildrenOnly):
                    i.deleteLater()

                if is_curr:
                    x1__=38
                    gap__=2
                    s_o_s= 14
                    x2__=s_o_s + gap__ + x1__
                    left_sticker_label=QLabel(msg_label)
                    left_sticker_label.setFixedSize(s_o_s,s_o_s)
                    left_sticker_label.move(x1__ , 3)
                    left_sticker_label.raise_()
                    left_sticker_label.show()

                    left_sticker_text_label=QLabel(msg_label)
                    left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                    left_sticker_text_label.setText('STICKER')
                    left_sticker_text_label.move(x2__,0)
                    left_sticker_text_label.adjustSize()
                    left_sticker_text_label.show()

                    i_fall_a_sleep=QLabel(msg_label)
                    i_fall_a_sleep.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color : rgb(182,182,182);background:transparent;''')
                    i_fall_a_sleep.setText('You : ')
                    i_fall_a_sleep.move(3,0)
                    i_fall_a_sleep.adjustSize()
                    i_fall_a_sleep.show()

                    self.set_svg_icon_and_color(left_sticker_label,'svg_icons/sticker.svg','#b6b6b6')

                else:
                    msg_label.setText("")
                    x1__=2
                    gap__=2
                    s_o_s=14
                    x2__=s_o_s + gap__ + x1__
                    left_sticker_label=QLabel(msg_label)
                    left_sticker_label.setFixedSize(s_o_s,s_o_s)
                    left_sticker_label.move(x1__,3)
                    left_sticker_label.raise_()
                    left_sticker_label.show()
                    left_sticker_text_label=QLabel(msg_label)
                    left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                    left_sticker_text_label.setText('STICKER')
                    left_sticker_text_label.move(x2__,0)
                    left_sticker_text_label.adjustSize()
                    left_sticker_text_label.show()

                self.friend_list_left_label_dict[dict_target_email]['last_msg_sent_at']=data['sent_at']
                self.friend_list_left_label_dict[dict_target_email]['last_msg_id']=data['_id']


            else:
                if last_time < data['sent_at']:
                    msg_label=self.friend_list_left_label_dict[dict_target_email]["label"].m_label
                    time_label=self.friend_list_left_label_dict[dict_target_email]['label'].t_label
                    self.set_text_to_stuff(time_label,datetime.fromisoformat(data['sent_at']).astimezone().strftime('%H:%M %p'))
                    msg_label.clear()

                    for i in msg_label.findChildren(QWidget,options=Qt.FindDirectChildrenOnly):
                        i.deleteLater()
                    if is_curr:
                        x1__=38
                        gap__=10
                        s_o_s=14
                        x2__=s_o_s + gap__ + x1__
                        left_sticker_label=QLabel(msg_label)
                        left_sticker_label.setFixedSize(s_o_s,s_o_s)
                        left_sticker_label.move(x1__,3)
                        left_sticker_label.raise_()
                        left_sticker_label.show()

                        left_sticker_text_label=QLabel(msg_label)
                        left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                        left_sticker_text_label.setText('STICKER')
                        left_sticker_text_label.move(x2__,0)
                        left_sticker_text_label.adjustSize()
                        left_sticker_text_label.show()

                        i_fall_a_sleep=QLabel(msg_label)
                        i_fall_a_sleep.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                        i_fall_a_sleep.setText('You : ')
                        i_fall_a_sleep.move(3,0)
                        i_fall_a_sleep.adjustSize()
                        i_fall_a_sleep.show()
                        self.set_svg_icon_and_color(left_sticker_label,'svg_icons/sticker.svg','#b6b6b6')

                    else:
                        msg_label.setText("")
                        x1__=2
                        gap__=2
                        s_o_s=14
                        x2__=s_o_s + gap__ + x1__
                        left_sticker_label=QLabel(msg_label)
                        left_sticker_label.setFixedSize(s_o_s,s_o_s)
                        left_sticker_label.move(x1__,3)
                        left_sticker_label.raise_()
                        left_sticker_label.show()
                        left_sticker_text_label=QLabel(msg_label)
                        left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                        left_sticker_text_label.setText('STICKER')
                        left_sticker_text_label.move(x2__,0)
                        left_sticker_text_label.adjustSize()
                        left_sticker_text_label.show()

                        self.set_svg_icon_and_color(left_sticker_label,'svg_icons/sticker.svg','#b6b6b6')
                    self.friend_list_left_label_dict[dict_target_email]['last_msg_sent_at']= data['sent_at']
                    self.friend_list_left_label_dict[dict_target_email]['last_msg_id']=data['_id']

        time_label=QLabel(sticker_label)
        time_label.setStyleSheet(theme['right_gif_sticke_time_ss']if is_curr else theme['left_gif_sticke_time_ss'])
        time_label.setFixedSize(64,20)
        local_time=datetime.fromisoformat(data["sent_at"]).astimezone()
        time_label.setText(f'{local_time.strftime("%I:%M %p")}')

        time_label.move(sticker_label.width()-time_label.width()-extra_height_for_time,sticker_label.height()-time_label.height())
        time_label.raise_()
        time_label.show()

        if not data.get('is_temp',False):
            transparent_long_bg_label.setFixedSize(max_main_bg_w+extra_height_for_time,sticker_label.height())
            sticker_label.setParent(transparent_long_bg_label)
            sticker_label.move(main_x,0)
            transparent_long_bg_label.setFixedSize(max_main_bg_w,sticker_label.height())

            button_size=30
            button_gap=3
            y_of_button=int((transparent_long_bg_label.height()-button_size)//2)

            if is_curr:
                button_x=main_x - int(button_size*4.5+button_gap*3)

                report_x=button_x
                delete_x=report_x + button_size + button_gap
                edit_x=delete_x + button_size + button_gap
                copy_x=edit_x + button_size + button_gap

            else:
                button_x=sticker_label.width() + int(button_size//2+button_gap)

                copy_x=button_x
                edit_x=copy_x + button_size + button_gap
                delete_x=edit_x + button_size + button_gap
                report_x= delete_x + button_size + button_gap
            report_button=QPushButton(transparent_long_bg_label)
            report_button.setGeometry(report_x,y_of_button,button_size,button_size)
            report_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(report_button,'svg_icons/chat_report.svg',self.current_chat_theme_styleSheet["chat_buttons_color"])

            report_button.setIconSize(QSize(int(button_size*0.5),int(button_size*0.5)))
            report_button.setCursor(Qt.PointingHandCursor)

            copy_button=QPushButton(transparent_long_bg_label)
            copy_button.setGeometry(copy_x,y_of_button,button_size,button_size)
            copy_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(copy_button,'svg_icons/chat_copy.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.6))
            copy_button.setIconSize(QSize(int(button_size*0.6),int(button_size*0.6)))
            copy_button.setCursor(Qt.PointingHandCursor)

            delete_button=QPushButton(transparent_long_bg_label)
            delete_button.setGeometry(delete_x,y_of_button,button_size,button_size)
            delete_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(delete_button,'svg_icons/chat_bin2.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.7))
            delete_button.setIconSize(QSize(int(button_size*0.7),int(button_size*0.7)))
            delete_button.setCursor(Qt.PointingHandCursor)

            edit_button=QPushButton(transparent_long_bg_label)
            edit_button.setGeometry(edit_x,y_of_button,button_size,button_size)
            edit_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(edit_button,'svg_icons/edit.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.6))
            edit_button.setIconSize(QSize(int(button_size*0.7),int(button_size*0.7)))
            edit_button.setCursor(Qt.PointingHandCursor)

            report_button.hide()
            copy_button.hide()
            delete_button.hide()
            edit_button.hide()

            def show_inside_buttons(event):
                report_button.show()
                copy_button.show()
                delete_button.show()
                edit_button.show()

            def hide_inside_buttons(event):
                report_button.hide()
                copy_button.hide()
                delete_button.hide()
                edit_button.hide()

            transparent_long_bg_label.enterEvent=show_inside_buttons
            transparent_long_bg_label.leaveEvent=hide_inside_buttons
            transparent_long_bg_label._id=data['_id']
            transparent_long_bg_label._type=data['msg_type']
            transparent_long_bg_label._msg_content= data['msg_content']
            transparent_long_bg_label._time=data['sent_at']
            transparent_long_bg_label._sent_by= data['sent_by']
            delete_button.clicked.connect(lambda:self.delete_a_chat_widget(data))

        if data.get('is_temp'):
            return sticker_label

        return transparent_long_bg_label

    def push_sticker_label_in_chat(self,label):
        try:
            item=QStandardItem()
            size=label.sizeHint()
            size.setHeight(size.height()+10)
            item.setSizeHint(size)
            self.chat_free_model.appendRow(item)
            index=self.chat_free_model.indexFromItem(item)
            if self.current_central_chat_frame=='first':
                self.chat_list_view_1.setIndexWidget(index ,label)
            else:
                self.chat_list_view_2.setIndexWidget(index,label)
            self.rebuild_day_labels()
        except Exception:
            print('error in push_sticker_label_in_chat:\n'+traceback.format_exc())



    def push_a_single_gif(self,data):
        data = self._view(data)
        self.ensure_day_label_for_today()
        label = self.create_sticker_msg_label(data)
        if label is None:
            return
        self.push_sticker_label_in_chat(label)


    def push_gif_label_in_chat(self,label):
        try:
            item=QStandardItem()
            size=label.sizeHint()
            size.setHeight(size.height()+10)
            item.setSizeHint(size)
            self.chat_free_model.appendRow(item)
            index=self.chat_free_model.indexFromItem(item)
            if self.current_central_chat_frame=='first':
                self.chat_list_view_1.setIndexWidget(index,label)
            else:
                self.chat_list_view_2.setIndexWidget(index,label)
            self.rebuild_day_labels()
        except Exception:
            print('error in push_gif_label_in_chat:\n'+traceback.format_exc())



    def create_gif_msg_label(self,data):

        max_main_bg_w=1100
        gif_width=230
        gif_height=230
        extra_height_for_time=20

        transparent_long_bg_label=QLabel()
        transparent_long_bg_label.setStyleSheet('background:transparent;')
        is_curr= self.current_user_email==data.get('sent_by',False)

        theme=self.current_chat_theme_styleSheet

        if is_curr:
            last_time =self.friend_list_left_label_dict[data['sent_to']]["last_msg_sent_at"]
            dict_target_email=data['sent_to']
        else:
            last_time=self.friend_list_left_label_dict[data['sent_by']]['last_msg_sent_at']
            dict_target_email=data['sent_by']

        if not data.get('is_temp',False):
            if last_time is None:
                msg_label=self.friend_list_left_label_dict[dict_target_email]["label"].m_label
                time_labl=self.friend_list_left_label_dict[dict_target_email]['label'].t_label
                self.set_text_to_stuff(time_labl,datetime.fromisoformat(data['sent_at']).astimezone().strftime('%H:%M %p'))
                msg_label.clear()

                for i in msg_label.findChildren(QWidget,options=Qt.FindDirectChildrenOnly):
                    i.deleteLater()

                if is_curr:
                    x1__=38
                    gap__=2
                    s_o_s=14
                    x2__=s_o_s + gap__ + x1__
                    left_sticker_label=QLabel(msg_label)
                    left_sticker_label.setFixedSize(s_o_s,s_o_s)
                    left_sticker_label.move(x1__,3)
                    left_sticker_label.raise_()
                    left_sticker_label.show()

                    left_sticker_text_label=QLabel(msg_label)
                    left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                    left_sticker_text_label.setText('GIF')
                    left_sticker_text_label.move(x2__ ,0)
                    left_sticker_text_label.adjustSize()
                    left_sticker_text_label.show()

                    i_fall_a_sleep=QLabel(msg_label)
                    i_fall_a_sleep.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight :400;color:rgb(182,182,182);background:transparent;''')
                    i_fall_a_sleep.setText('You : ')
                    i_fall_a_sleep.move(3 ,0)
                    i_fall_a_sleep.adjustSize()
                    i_fall_a_sleep.show()

                    self.set_svg_icon_and_color(left_sticker_label,'svg_icons/gif.svg','#b6b6b6')

                else:
                    msg_label.setText("")
                    x1__=2
                    gap__=2
                    s_o_s= 14
                    x2__=s_o_s + gap__ + x1__
                    left_sticker_label=QLabel(msg_label)
                    left_sticker_label.setFixedSize(s_o_s,s_o_s)
                    left_sticker_label.move(x1__,3)
                    left_sticker_label.raise_()
                    left_sticker_label.show()
                    left_sticker_text_label=QLabel(msg_label)
                    left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size :14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                    left_sticker_text_label.setText('GIF')
                    left_sticker_text_label.move(x2__ ,0)
                    left_sticker_text_label.adjustSize()
                    left_sticker_text_label.show()
                    self.set_svg_icon_and_color(left_sticker_label,'svg_icons/gif.svg','#b6b6b6')

                self.friend_list_left_label_dict[dict_target_email]['last_msg_sent_at']=data['sent_at']


            else:
                if last_time < data['sent_at']:
                    msg_label =self.friend_list_left_label_dict[dict_target_email]["label"].m_label
                    time_label = self.friend_list_left_label_dict[dict_target_email]['label'].t_label
                    self.set_text_to_stuff(time_label,datetime.fromisoformat(data['sent_at']).astimezone().strftime('%H:%M %p'))
                    msg_label.clear()

                    for i in msg_label.findChildren(QWidget,options=Qt.FindDirectChildrenOnly):
                        i.deleteLater()
                    if is_curr:
                        x1__=38
                        gap__=3
                        s_o_s=14
                        x2__=s_o_s + gap__ + x1__
                        left_sticker_label=QLabel(msg_label)
                        left_sticker_label.setFixedSize(s_o_s,s_o_s)
                        left_sticker_label.move(x1__,3)
                        left_sticker_label.raise_()
                        left_sticker_label.show()

                        left_sticker_text_label =QLabel(msg_label)
                        left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                        left_sticker_text_label.setText('GIF')
                        left_sticker_text_label.move(x2__,0)
                        left_sticker_text_label.adjustSize()
                        left_sticker_text_label.show()

                        i_fall_a_sleep=QLabel(msg_label)
                        i_fall_a_sleep.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                        i_fall_a_sleep.setText('You : ')
                        i_fall_a_sleep.move(3,0)
                        i_fall_a_sleep.adjustSize()
                        i_fall_a_sleep.show()
                        self.set_svg_icon_and_color(left_sticker_label,'svg_icons/gif.svg','#b6b6b6')

                    else:
                        msg_label.setText("")
                        x1__=2
                        gap__ =2
                        s_o_s=14
                        x2__=s_o_s + gap__ + x1__
                        left_sticker_label=QLabel(msg_label)
                        left_sticker_label.setFixedSize(s_o_s,s_o_s)
                        left_sticker_label.move(x1__,3)
                        left_sticker_label.raise_()
                        left_sticker_label.show()
                        left_sticker_text_label=QLabel(msg_label)
                        left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                        left_sticker_text_label.setText('GIF')
                        left_sticker_text_label.move(x2__,0)
                        left_sticker_text_label.adjustSize()
                        left_sticker_text_label.show()

                        self.set_svg_icon_and_color(left_sticker_label,'svg_icons/gif.svg','#b6b6b6')
                    self.friend_list_left_label_dict[dict_target_email]['last_msg_sent_at']=data['sent_at']
                    self.friend_list_left_label_dict[dict_target_email]['last_msg_id']=data['_id']
        gif_label=QLabel()
        gif_label.setFixedSize(gif_width,gif_height)
        gif_label.setAlignment(Qt.AlignCenter)
        gif_label.setStyleSheet("background: transparent;")

        gif_movie=QMovie(data["msg_content"])
        gif_movie.setScaledSize(QSize(180,180))
        gif_label.setMovie(gif_movie)
        gif_label.setMovie(gif_movie)
        gif_movie.start()
        gif_label.movie_=gif_movie

        if is_curr:
            main_x=max_main_bg_w - gif_label.width() - 20

        else:
            main_x= 10

        gif_label.move(main_x,0)

        time_label= QLabel(gif_label)
        time_label.setStyleSheet(theme['right_gif_sticke_time_ss']if is_curr else theme['left_gif_sticke_time_ss'])
        time_label.setFixedSize(64,20)
        local_time=datetime.fromisoformat(data["sent_at"]).astimezone()
        time_label.setText(f'{local_time.strftime("%I:%M %p")}')

        time_label.move(gif_label.width()-time_label.width()-extra_height_for_time,gif_label.height()-time_label.height())
        time_label.raise_()

        if not data.get('is_temp',False):
            transparent_long_bg_label.setFixedSize(max_main_bg_w+extra_height_for_time,gif_label.height())
            gif_label.setParent(transparent_long_bg_label)
            gif_label.move(main_x,0)
            transparent_long_bg_label.setFixedSize(max_main_bg_w,gif_label.height())

            button_size=30
            button_gap=3
            y_of_button=int((transparent_long_bg_label.height()-button_size)//2)

            if is_curr:
                button_x=main_x - int(button_size*4.5+button_gap*3)

                report_x = button_x
                delete_x= report_x + button_size + button_gap
                edit_x=delete_x + button_size + button_gap
                copy_x=edit_x + button_size + button_gap

            else:
                button_x=gif_label.width() + int(button_size//2+button_gap)

                copy_x = button_x
                edit_x=copy_x + button_size + button_gap
                delete_x =edit_x + button_size + button_gap
                report_x=delete_x + button_size + button_gap
            report_button=QPushButton(transparent_long_bg_label)
            report_button.setGeometry(report_x,y_of_button,button_size,button_size)
            report_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(report_button,'svg_icons/chat_report.svg',self.current_chat_theme_styleSheet["chat_buttons_color"])

            report_button.setIconSize(QSize(int(button_size*0.5),int(button_size*0.5)))
            report_button.setCursor(Qt.PointingHandCursor)

            copy_button=QPushButton(transparent_long_bg_label)
            copy_button.setGeometry(copy_x,y_of_button,button_size,button_size)
            copy_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(copy_button,'svg_icons/chat_copy.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.6))
            copy_button.setIconSize(QSize(int(button_size*0.6),int(button_size*0.6)))
            copy_button.setCursor(Qt.PointingHandCursor)

            delete_button=QPushButton(transparent_long_bg_label)
            delete_button.setGeometry(delete_x,y_of_button,button_size,button_size)
            delete_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(delete_button,'svg_icons/chat_bin2.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.7))
            delete_button.setIconSize(QSize(int(button_size*0.7),int(button_size*0.7)))
            delete_button.setCursor(Qt.PointingHandCursor)

            edit_button=QPushButton(transparent_long_bg_label)
            edit_button.setGeometry(edit_x,y_of_button,button_size,button_size)
            edit_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(edit_button,'svg_icons/edit.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.6))
            edit_button.setIconSize(QSize(int(button_size*0.7),int(button_size*0.7)))
            edit_button.setCursor(Qt.PointingHandCursor)

            report_button.hide()
            copy_button.hide()
            delete_button.hide()
            edit_button.hide()

            def show_inside_buttons(event):
                report_button.show()
                copy_button.show()
                delete_button.show()
                edit_button.show()

            def hide_inside_buttons(event):
                report_button.hide()
                copy_button.hide()
                delete_button.hide()
                edit_button.hide()

            transparent_long_bg_label.enterEvent =show_inside_buttons
            transparent_long_bg_label.leaveEvent=hide_inside_buttons
            transparent_long_bg_label._id=data['_id']
            transparent_long_bg_label._type=data['msg_type']
            transparent_long_bg_label._msg_content=data['msg_content']
            transparent_long_bg_label._time=data['sent_at']
            transparent_long_bg_label._sent_by=data['sent_by']
            delete_button.clicked.connect(lambda:self.delete_a_chat_widget(data))

        if data.get('is_temp'):
            return gif_label

        transparent_long_bg_label.setFixedSize(max_main_bg_w+extra_height_for_time,gif_label.height())
        return transparent_long_bg_label


    
    def push_a_single_delete_from_everyone_direct(self, data):
        try:
            self.ensure_day_label_for_today()
            data = self._view(data)
            data['msg_type'] = 'delete_from_everyone'
            data['delete_from_all'] = True
            data['is_temp'] = False
            label = self.create_delete_from_every_one_label(data)
            if label is None:
                return
            item = QStandardItem()
            size = label.sizeHint()
            size.setHeight(size.height() + 10)
            item.setSizeHint(size)
            self.chat_free_model.appendRow(item)
            index = self.chat_free_model.indexFromItem(item)
            if self.current_central_chat_frame == 'first':
                self.chat_list_view_1.setIndexWidget(index, label)
            else:
                self.chat_list_view_2.setIndexWidget(index, label)
            self.rebuild_day_labels()
        except Exception:
            print('error in push_a_single_delete_from_everyone_direct:\n' + traceback.format_exc())


    
    def _day_key_from_iso(self,iso_ts):
        try:
            return datetime.fromisoformat(iso_ts).astimezone().strftime("%Y-%m-%d")
        except Exception:
            return None

    def rebuild_day_labels(self):
        if getattr(self,'_suspend_day_rebuild',False):
            return
        try:
            model= getattr(self,'chat_free_model',None)
            if model is None:
                return
            list_view=(self.chat_list_view_1 if self.current_central_chat_frame=='first' else self.chat_list_view_2)


            for row in range(model.rowCount()-1,-1,-1):
                item=model.item(row)
                if item is None:
                    continue
                idx=model.indexFromItem(item)
                w=list_view.indexWidget(idx)
                if w is not None and getattr(w,'_is_day_label',False):
                    w.deleteLater()
                    model.removeRow(row)


            prev_day=None
            row =0
            while row < model.rowCount():
                item=model.item(row)
                if item is None:
                    row+=1
                    continue
                idx=model.indexFromItem(item)
                w=list_view.indexWidget(idx)
                t=getattr(w,'_time',None)
                if not t:
                    row+=1
                    continue

                day=self._day_key_from_iso(t)
                if day is None:
                    row+=1
                    continue

                if day!=prev_day:
                    label=self.create_top_day_time_label(day)
                    new_item=QStandardItem()
                    new_item.setSizeHint(label.sizeHint())
                    model.insertRow(row,new_item)
                    list_view.setIndexWidget(model.indexFromItem(new_item),label)
                    prev_day=day
                    row+=2
                else:
                    row+=1
        except Exception:
            print('error in rebuild_day_labels:\n'+traceback.format_exc())

    def create_delete_from_every_one_label(self,data):

        max_main_bg_w=1100

        extra_height_for_time= 10

        transparent_long_bg_label = QLabel()
        transparent_long_bg_label.setStyleSheet('background:transparent;')
        is_curr=self.current_user_email==data.get('sent_by',False)

        if is_curr:
            last_time=self.friend_list_left_label_dict[data['sent_to']]["last_msg_sent_at"]
            dict_target_email=data['sent_to']
        else:
            last_time=self.friend_list_left_label_dict[data['sent_by']]['last_msg_sent_at']
            dict_target_email=data['sent_by']

        if last_time is not None and data['_id']==self.friend_list_left_label_dict[dict_target_email].get('last_msg_id'):
            msg_label=self.friend_list_left_label_dict[dict_target_email]["label"].m_label
            time_labl=self.friend_list_left_label_dict[dict_target_email]['label'].t_label
            self.set_text_to_stuff(time_labl,datetime.fromisoformat(data['sent_at']).astimezone().strftime('%H:%M %p'))
            msg_label.clear()

            for i in msg_label.findChildren(QWidget,options=Qt.FindDirectChildrenOnly):
                i.deleteLater()
            if is_curr:
                x1__=38
                gap__=2
                s_o_s=14
                x2__=s_o_s + gap__ + x1__
                left_sticker_label=QLabel(msg_label)
                left_sticker_label.setFixedSize(s_o_s, s_o_s)
                left_sticker_label.move(x1__,3)
                left_sticker_label.raise_()
                left_sticker_label.show()

                left_sticker_text_label=QLabel(msg_label)
                left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182) ; background:transparent;''')
                left_sticker_text_label.setText('Deleted Message')
                left_sticker_text_label.move(x2__ ,0)
                left_sticker_text_label.adjustSize()
                left_sticker_text_label.show()

                i_fall_a_sleep=QLabel(msg_label)
                i_fall_a_sleep.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                i_fall_a_sleep.setText('You : ')
                i_fall_a_sleep.move(3,0)
                i_fall_a_sleep.adjustSize()
                i_fall_a_sleep.show()

                self.set_svg_icon_and_color(left_sticker_label,'svg_icons/delete.svg','#b6b6b6')

            else:
                msg_label.setText("")
                x1__=2
                gap__=2
                s_o_s= 14
                x2__=s_o_s + gap__ + x1__
                left_sticker_label= QLabel(msg_label)
                left_sticker_label.setFixedSize(s_o_s,s_o_s)
                left_sticker_label.move(x1__,3)
                left_sticker_label.raise_()
                left_sticker_label.show()
                left_sticker_text_label=QLabel(msg_label)
                left_sticker_text_label.setStyleSheet('''font-family:"Inter","Segoe UI";font-size:14px;font-weight:400;color:rgb(182,182,182);background:transparent;''')
                left_sticker_text_label.setText('Deleted Meage')
                left_sticker_text_label.move(x2__,0)
                left_sticker_text_label.adjustSize()
                left_sticker_text_label.show()
                self.set_svg_icon_and_color(left_sticker_label , 'svg_icons/delete.svg','#b6b6b6')

        theme=self.current_chat_theme_styleSheet
        transparent_long_bg_label.setFixedSize(max_main_bg_w,1)

        bubble_stylesheet=theme['current_user_stylesheet'] if is_curr else theme['receiver_user_stylesheet']

        provied_text="This message was deleted from everyone"

        temp_label_for_size= QLabel()
        temp_label_for_size.setStyleSheet(bubble_stylesheet)
        temp_label_for_size.setText(provied_text)
        temp_label_for_size.setWordWrap(True)
        temp_label_for_size.ensurePolished()

        fm=temp_label_for_size.fontMetrics()

        margins_=temp_label_for_size.contentsMargins()
        extra_w=margins_.left() + margins_.right()

        extra_h=margins_.top() + margins_.bottom()

        rect=fm.boundingRect(0,0,10000,10000,Qt.TextWordWrap|Qt.AlignLeft|Qt.AlignTop,provied_text)

        width=rect.width() + extra_w
        height=rect.height() + extra_h * 2
        main_content_bg=QLabel()
        main_content_bg.setStyleSheet(bubble_stylesheet)
        main_content_bg.setAlignment(Qt.AlignTop)
        main_content_bg.setFixedSize(width+margins_.left()+10,int(height+extra_height_for_time//2))
        main_content_bg.setText(provied_text)
        temp_label_for_size.setFixedSize(int(width),int(height))

        time_label=QLabel(main_content_bg)
        time_label.setStyleSheet(theme['right_text_time_stylesheet']if is_curr else theme['left_text_time_stylesheet'])
        time_label.setFixedSize(41,15)
        time_x =int(main_content_bg.width()-time_label.width()-margins_.left())
        time_y=int(main_content_bg.height()-extra_height_for_time*1.8)
        time_label.move(time_x ,time_y)
        local_time=datetime.fromisoformat(data["sent_at"]).astimezone()
        time_label.setText(f'{local_time.strftime("%I:%M %p")}')

        if not data.get('is_temp',False):
            if is_curr:
                main_x=max_main_bg_w - main_content_bg.width() - 20
            else:
                main_x=10
            main_content_bg.setParent(transparent_long_bg_label)
            main_content_bg.move(main_x,0)
            main_content_bg.show()
            transparent_long_bg_label.setFixedSize(max_main_bg_w,main_content_bg.height())

            button_size=30
            button_gap=3
            y_of_button=int((transparent_long_bg_label.height()-button_size)//2)

            if is_curr:
                button_x=main_x - int(button_size*4.5+button_gap*3)

                report_x=button_x
                delete_x=report_x + button_size + button_gap
                edit_x=delete_x + button_size + button_gap
                copy_x=edit_x + button_size + button_gap

            else:
                button_x = main_content_bg.width() + int(button_size//2+button_gap)

                copy_x=button_x
                edit_x = copy_x + button_size + button_gap
                delete_x=edit_x + button_size + button_gap
                report_x=delete_x + button_size + button_gap

            report_button=QPushButton(transparent_long_bg_label)
            report_button.setGeometry(report_x,y_of_button,button_size,button_size)
            report_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(report_button,'svg_icons/chat_report.svg',self.current_chat_theme_styleSheet["chat_buttons_color"])

            report_button.setIconSize(QSize(int(button_size*0.5),int(button_size*0.5)))
            report_button.setCursor(Qt.PointingHandCursor)

            copy_button=QPushButton(transparent_long_bg_label)
            copy_button.setGeometry(copy_x,y_of_button, button_size,button_size)
            copy_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(copy_button,'svg_icons/chat_copy.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.6))
            copy_button.setIconSize(QSize(int(button_size*0.6),int(button_size*0.6)))
            copy_button.setCursor(Qt.PointingHandCursor)

            delete_button=QPushButton(transparent_long_bg_label)
            delete_button.setGeometry(delete_x,y_of_button,button_size,button_size)
            delete_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(delete_button,'svg_icons/chat_bin2.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.7))
            delete_button.setIconSize(QSize(int(button_size*0.7), int(button_size*0.7)))
            delete_button.setCursor(Qt.PointingHandCursor)

            edit_button=QPushButton(transparent_long_bg_label)
            edit_button.setGeometry(edit_x,y_of_button,button_size,button_size)
            edit_button.setStyleSheet(self.current_chat_theme_styleSheet['chat_buttons_styleSheet'])
            self.set_svg_icon_and_color(edit_button,'svg_icons/edit.svg',self.current_chat_theme_styleSheet["chat_buttons_color"],int(button_size*0.6))
            edit_button.setIconSize(QSize(int(button_size*0.7),int(button_size*0.7)))
            edit_button.setCursor(Qt.PointingHandCursor)

            report_button.hide()
            copy_button.hide()
            delete_button.hide()
            edit_button.hide()

            def show_inside_buttons(event):
                report_button.show()
                copy_button.show()
                delete_button.show()
                edit_button.show()

            def hide_inside_buttons(event):
                report_button.hide()
                copy_button.hide()
                delete_button.hide()
                edit_button.hide()

            transparent_long_bg_label.enterEvent=show_inside_buttons
            transparent_long_bg_label.leaveEvent=hide_inside_buttons
            transparent_long_bg_label._id=data['_id']
            transparent_long_bg_label._type=data['msg_type']
            transparent_long_bg_label._msg_content=data['msg_content']
            transparent_long_bg_label._time=data['sent_at']
            transparent_long_bg_label._sent_by=data['sent_by']

            delete_button.clicked.connect(lambda:self.delete_a_chat_widget(data))

            return transparent_long_bg_label

        if data.get('is_temp'):
            return main_content_bg
if __name__=="__main__":

    def wipe_all_local_data(db_paths=("messages.db","last_seen.db","last_clicked.db","last_message_preview.db") ,media_folders=("thumbnail_images","user_images")):

        table_map={"messages.db":"messages","last_seen.db" :"last_seen","last_clicked.db":"chat_last_clicked",}

        for db_file in db_paths:
            table=table_map.get(db_file)
            if not table:
                if db_file=="last_message_preview.db":
                    continue
                continue
            try:
                if os.path.exists(db_file):
                    conn=sqlite3.connect(db_file)
                    try:
                        conn.execute(f"DELETE FROM {table}")
                        conn.commit()
                    finally:
                        conn.close()
            except Exception as e:
                print(f'[wipe] error clearing table in {db_file}: {e}')

        for db_file in db_paths:
            try:
                if os.path.exists(db_file):
                    os.remove(db_file)
                    print(f'[wipe] removed {db_file}')
            except Exception as e:
                print(f'[wipe] could not remove {db_file}: {e}')

        for folder in media_folders:
            try:
                if os.path.isdir(folder):
                    for fname in os.listdir(folder):
                        fpath=os.path.join(folder,fname)
                        try:
                            if os.path.isfile(fpath):
                                os.remove(fpath)
                        except Exception as e:
                            print(f'[wipe] could not remove {fpath}: {e}')

            except Exception as e:
                print(f'[wipe] error wiping folder {folder}: {e}')

        print('[wipe] local data wipe complete')
        print('[wipe] wiped databases: messages.db, last_seen.db, last_clicked.db, last_message_preview.db')
        print('[wipe] wiped folders: thumbnail_images, user_images')

    print(datetime.now())
    wipe_all_local_data()
    app=QApplication(sys.argv)

    widget=QStackedWidget()
    main_wind = chating_main()

    widget.addWidget(main_wind)
    widget.showMaximized()

    sys.exit(app.exec())
