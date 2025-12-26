import socket
import threading
import json
import time
import traceback
from .data_manager import ensure_cp_exists, update_cp, get_all_cps, load_data, get_cp

class CPSocketServer:
    """
    Servidor TCP que acepta conexiones de CP monitors.
    CON DETECCIÓN DE CLAVE CORRUPTA (sin cerrar conexión)
    """

    def __init__(self, host="0.0.0.0", port=5001, producer=None, socketio=None, 
                 crypto_manager=None, audit_logger=None):
        self.host = host
        self.port = port
        self.producer = producer
        self.socketio = socketio
        self.crypto_manager = crypto_manager
        self.audit_logger = audit_logger
        self._sock = None
        self._clients = {}
        self._stop_flag = threading.Event()
        self._clients_lock = threading.Lock()

    def start(self):
        t = threading.Thread(target=self._serve_forever, daemon=True)
        t.start()

    def stop(self):
        self._stop_flag.set()
        if self._sock:
            try:
                self._sock.close()
            except:
                pass

    def _notify_web(self, message, msg_type='info'):
        """Envía notificación al panel web vía WebSocket"""
        if self.socketio:
            try:
                self.socketio.emit('notification', {
                    'type': msg_type,
                    'message': message
                }, namespace='/')
            except Exception as e:
                print(f"[CP SOCKET] Error enviando notificación web: {e}")

    def send_command_to_cp(self, cp_id, command):
        """Envía un comando a un CP específico"""
        with self._clients_lock:
            if cp_id not in self._clients:
                print(f"[CP SOCKET] ⚠️  CP {cp_id} no está conectado")
                return False
            
            conn, addr = self._clients[cp_id]
        
        try:
            msg = json.dumps(command) + "\n"
            conn.sendall(msg.encode('utf-8'))
            print(f"[CP SOCKET] ✅ Comando enviado a {cp_id}: {command}")
            return True
        except Exception as e:
            print(f"[CP SOCKET] ❌ Error enviando comando a {cp_id}: {e}")
            with self._clients_lock:
                if cp_id in self._clients:
                    del self._clients[cp_id]
            return False

    def _serve_forever(self):
        print(f"[CP SOCKET] 🔌 Iniciando servidor en {self.host}:{self.port}")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(100)
        self._sock = s
        try:
            while not self._stop_flag.is_set():
                conn, addr = s.accept()
                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()
        except Exception as e:
            print("[CP SOCKET] Error en server loop:", e)
        finally:
            s.close()

    def _verify_credentials(self, cp_id, username, password):
        """Verifica las credenciales del CP"""
        if username == cp_id and password == "default_password":
            return True
        return False

    def _handle_client(self, conn, addr):
        buff = b""
        cp_id = None
        authenticated = False
        encryption_key = None
        
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buff += data
                
                while True:
                    try:
                        text = buff.decode("utf-8").strip()
                    except UnicodeDecodeError:
                        break
                    if not text:
                        buff = b""
                        break
                    
                    try:
                        if "\n" in text:
                            first, rest = text.split("\n", 1)
                            obj = json.loads(first.strip())
                            buff = rest.encode("utf-8")
                        else:
                            obj = json.loads(text)
                            buff = b""
                    except json.JSONDecodeError:
                        break

                    # === DESCIFRAR si está autenticado ===
                    if authenticated and "encrypted" in obj:
                        try:
                            from cryptography.fernet import Fernet
                            encrypted_data = obj["encrypted"]
                            decryption_key = self.crypto_manager.get_key_for_cp(cp_id)
                            
                            if not decryption_key:
                                print(f"[CP SOCKET] 🔴 ERROR: No hay clave para {cp_id}")
                                
                                # ✅ NO CERRAR - Solo enviar error
                                error_msg = {
                                    "type": "key_validation_ack",
                                    "status": "error",
                                    "reason": "No encryption key found"
                                }
                                conn.sendall((json.dumps(error_msg) + "\n").encode('utf-8'))
                                continue
                            
                            fernet = Fernet(decryption_key.encode('utf-8'))
                            decrypted = fernet.decrypt(encrypted_data.encode('utf-8'))
                            obj = json.loads(decrypted.decode('utf-8'))
                            
                        except Exception as e:
                            print("="*70)
                            print(f"[CP SOCKET] 🔴🔴🔴 ERROR DESCIFRANDO MENSAJE DE {cp_id} 🔴🔴🔴")
                            print(f"[CP SOCKET] ⚠️  Tipo de error: {type(e).__name__}")
                            print(f"[CP SOCKET] ⚠️  Detalle: {e}")
                            print(f"[CP SOCKET] 🔑 CLAVE CORRUPTA O INVÁLIDA")
                            print("="*70)
                            
                            # Auditar fallo
                            if self.audit_logger:
                                self.audit_logger.log_decryption_failed(cp_id, addr[0])
                            
                            # Notificar panel web
                            self._notify_web(f"🔴 CP {cp_id} - CLAVE DE CIFRADO CORRUPTA", 'error')
                            
                            # ✅ NO CERRAR - Enviar error al Monitor
                            error_msg = {
                                "type": "key_validation_ack",
                                "status": "error",
                                "reason": "Decryption failed - Invalid or corrupted key",
                                "error_type": type(e).__name__
                            }
                            try:
                                conn.sendall((json.dumps(error_msg) + "\n").encode('utf-8'))
                            except:
                                pass
                            
                            # ✅ CONTINUAR sin romper el bucle
                            continue

                    # === PROCESAR MENSAJE ===
                    mtype = obj.get("type")
                    
                    # === VALIDACIÓN DE CLAVE (HEARTBEAT) ===
                    if mtype == "key_validation_ping":
                        # Si llegamos aquí, el descifrado fue exitoso → clave válida
                        print(f"[CP SOCKET] ✅ Validación de clave OK para {cp_id}")
                        ack = {
                            "type": "key_validation_ack",
                            "status": "ok",
                            "timestamp": time.time()
                        }
                        try:
                            conn.sendall((json.dumps(ack) + "\n").encode('utf-8'))
                        except:
                            pass
                        continue
                    
                    # === AUTENTICACIÓN ===
                    if mtype == "auth":
                        cp_id = obj.get("cp_id")
                        username = obj.get("username")
                        password = obj.get("password")
                        location = obj.get("location", "Unknown")
                        price = obj.get("price", 0.35)
                        
                        print(f"[CP SOCKET] 🔐 Solicitud de autenticación de {cp_id} desde {addr}")
                        
                        if not self._verify_credentials(cp_id, username, password):
                            print(f"[CP SOCKET] ❌ Credenciales inválidas para {cp_id}")
                            
                            if self.audit_logger:
                                self.audit_logger.log_auth_failure(cp_id, addr[0], 
                                                                   "Credenciales inválidas")
                            
                            response = {"type": "auth_ack", "status": "denied", "reason": "Credenciales inválidas"}
                            conn.sendall((json.dumps(response) + "\n").encode('utf-8'))
                            continue
                        
                        # Credenciales OK
                        ensure_cp_exists(cp_id, location=location, price=price)
                        
                        # Generar o recuperar clave
                        if self.crypto_manager:
                            if not self.crypto_manager.has_key(cp_id):
                                encryption_key = self.crypto_manager.generate_key_for_cp(cp_id)
                                print(f"[CP SOCKET] 🔑 Nueva clave generada para {cp_id}")
                                
                                if self.audit_logger:
                                    self.audit_logger.log_key_generated(cp_id, addr[0])
                            else:
                                encryption_key = self.crypto_manager.get_key_for_cp(cp_id)
                                print(f"[CP SOCKET] 🔑 Clave existente recuperada para {cp_id}")
                        
                        authenticated = True
                        update_cp(cp_id, state="ACTIVADO", location=location, price=float(price))
                        
                        print(f"[CP SOCKET] ✅ {cp_id} autenticado correctamente")
                        
                        if self.audit_logger:
                            self.audit_logger.log_auth_success(cp_id, addr[0])
                        
                        with self._clients_lock:
                            self._clients[cp_id] = (conn, addr)
                        
                        self._notify_web(f"CP {cp_id} autenticado y ACTIVADO", 'success')
                        
                        response = {
                            "type": "auth_ack",
                            "status": "ok",
                            "encryption_key": encryption_key
                        }
                        conn.sendall((json.dumps(response) + "\n").encode('utf-8'))
                        continue
                    
                    # === REGISTRO ===
                    if mtype == "register":
                        cp_id = obj.get("cp_id")
                        location = obj.get("location", "Unknown")
                        price = obj.get("price_eur_kwh", obj.get("price", 0.0))
                        
                        ensure_cp_exists(cp_id, location=location, price=price)
                        update_cp(cp_id, state="ACTIVADO", location=location, price=float(price))
                        
                        print(f"[CP SOCKET] ✅ CP registrado: {cp_id} @ {location}")
                        
                        with self._clients_lock:
                            self._clients[cp_id] = (conn, addr)
                        
                        self._notify_web(f"CP {cp_id} registrado y ACTIVADO", 'success')
                        
                        ack = {"type": "register_ack", "status": "ok"}
                        try:
                            conn.sendall((json.dumps(ack) + "\n").encode("utf-8"))
                        except:
                            pass
                        continue
                    
                    # === CAMBIO DE ESTADO ===
                    elif mtype == "status_change":
                        new_state = obj.get("state")
                        update_cp(cp_id, state=new_state)
                        print(f"[CP SOCKET] 🔄 Estado de {cp_id}: {new_state}")
                        continue
                    
                    # === AVERÍA ===
                    elif mtype == "fault":
                        fault_msg = obj.get("msg", "Unknown fault")
                        update_cp(cp_id, state="AVERIADO")
                        
                        print(f"[CP SOCKET] 🔴 AVERÍA en {cp_id}: {fault_msg}")
                        
                        if self.audit_logger:
                            self.audit_logger.log_cp_fault(cp_id, addr[0], fault_msg)
                        
                        self._notify_web(f"⚠️ AVERÍA en {cp_id}", 'error')
                        
                        if self.producer:
                            self.producer.publish_monitor({
                                "type": "fault",
                                "cp_id": cp_id,
                                "msg": fault_msg
                            })
                    
                    # === DESCONEXIÓN ===
                    elif mtype == "disconnect":
                        update_cp(cp_id, state="DESCONECTADO")
                        print(f"[CP SOCKET] ⚫ CP {cp_id} desconectado")
                        self._notify_web(f"CP {cp_id} desconectado", 'info')
                        break
                    
                    # === ACK DE COMANDO ===
                    elif mtype == "command_ack":
                        action = obj.get("action")
                        status = obj.get("status")
                        print(f"[CP SOCKET] 📨 ACK de {cp_id}: {action} -> {status}")

        except Exception as e:
            print(f"[CP SOCKET] ❌ Error: {e}")
            traceback.print_exc()
        
        finally:
            if cp_id:
                try:
                    update_cp(
                        cp_id, 
                        state="DESCONECTADO",
                        current_kw=0.0,
                        total_kwh=0.0,
                        current_euros=0.0,
                        current_driver=None
                    )
                    print(f"[CP SOCKET] 📴 CP {cp_id} marcado como DESCONECTADO")
                    self._notify_web(f"CP {cp_id} desconectado", 'info')
                except Exception as e:
                    print(f"[CP SOCKET] ❌ Error: {e}")
                
                with self._clients_lock:
                    if cp_id in self._clients:
                        del self._clients[cp_id]
            
            try:
                conn.close()
            except:
                pass
            
            print(f"[CP SOCKET] 🔌 Conexión finalizada {addr}")