#!/usr/bin/env python3
"""
EV_CP_M - Monitor del punto de recarga (CON DETECCIÓN DE CLAVE CORRUPTA)
- Autenticación con Central
- Cifrado de mensajes
- Health checks al Engine
- ✅ VALIDACIÓN DE CLAVE integrada (cada 5 seg)
- ✅ DETECCIÓN DE CLAVE CORRUPTA sin cerrar conexión
"""

import socket
import time
import os
import json
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils.helper import log_message

# Configuración
CP_ID = os.getenv('CP_ID', 'CP001')
CP_LOCATION = os.getenv('CP_LOCATION', 'Valencia Centro')
CP_PRICE = float(os.getenv('CP_PRICE', '0.35'))

ENGINE_HOST = os.getenv('ENGINE_HOST', 'localhost')
ENGINE_MONITOR_PORT = int(os.getenv('ENGINE_MONITOR_PORT', 9101))

CENTRAL_HOST = os.getenv('CENTRAL_HOST', 'localhost')
CENTRAL_PORT = int(os.getenv('CENTRAL_PORT', 5001))

# Credenciales
CP_USERNAME = os.getenv('CP_USERNAME', CP_ID)
CP_PASSWORD = os.getenv('CP_PASSWORD', 'default_password')

# Estado
sock_central = None
registered = False
encryption_key = None

def connect_to_central():
    """Conecta y autentica con Central"""
    global sock_central, registered, encryption_key
    
    max_retries = 5
    for retry in range(max_retries):
        try:
            log_message(f'[MONITOR] 🔄 Conectando a Central ({CENTRAL_HOST}:{CENTRAL_PORT})...')
            
            sock_central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_central.settimeout(10)
            sock_central.connect((CENTRAL_HOST, CENTRAL_PORT))
            
            # Autenticar
            auth_msg = {
                "type": "auth",
                "cp_id": CP_ID,
                "username": CP_USERNAME,
                "password": CP_PASSWORD,
                "location": CP_LOCATION,
                "price": CP_PRICE
            }
            
            msg_str = json.dumps(auth_msg) + "\n"
            sock_central.sendall(msg_str.encode('utf-8'))
            
            # Esperar respuesta
            response = sock_central.recv(4096).decode('utf-8').strip()
            if response:
                ack = json.loads(response)
                if ack.get("type") == "auth_ack" and ack.get("status") == "ok":
                    encryption_key = ack.get("encryption_key")
                    log_message(f'[MONITOR] ✅ Autenticado: {CP_ID}')
                    
                    if encryption_key:
                        log_message(f'[MONITOR] 🔑 Clave recibida: {encryption_key[:16]}...')
                        save_credentials(encryption_key)
                    else:
                        log_message(f'[MONITOR] ⚠️  No se recibió clave de cifrado')
                    
                    registered = True
                    return True
                else:
                    reason = ack.get("reason", "Unknown")
                    log_message(f'[MONITOR] ❌ Denegado: {reason}')
            
            sock_central.close()
            
        except Exception as e:
            log_message(f'[MONITOR] ❌ Error (intento {retry+1}/{max_retries}): {e}')
            if sock_central:
                sock_central.close()
        
        time.sleep(5)
    
    return False

def save_credentials(key):
    """Guarda la clave de cifrado en archivo"""
    try:
        credentials = {
            "cp_id": CP_ID,
            "encryption_key": key,
            "timestamp": time.time()
        }
        with open('credentials.json', 'w') as f:
            json.dump(credentials, f, indent=2)
        log_message('[MONITOR] 💾 Credenciales guardadas')
    except Exception as e:
        log_message(f'[MONITOR] ⚠️  Error guardando credenciales: {e}')

def encrypt_message(message):
    """Cifra un mensaje con la clave actual"""
    try:
        from cryptography.fernet import Fernet
        fernet = Fernet(encryption_key.encode('utf-8'))
        msg_str = json.dumps(message)
        encrypted = fernet.encrypt(msg_str.encode('utf-8'))
        return {"encrypted": encrypted.decode('utf-8')}
    except Exception as e:
        log_message(f'[MONITOR] ❌ Error cifrando: {e}')
        return None

def send_to_central(message_dict):
    """Envía mensaje a Central (cifrado si hay clave)"""
    global sock_central
    
    try:
        if encryption_key:
            encrypted_msg = encrypt_message(message_dict)
            if not encrypted_msg:
                return False
            final_msg = json.dumps(encrypted_msg) + "\n"
        else:
            final_msg = json.dumps(message_dict) + "\n"
        
        sock_central.sendall(final_msg.encode('utf-8'))
        return True
    except Exception as e:
        log_message(f'[MONITOR] Error enviando: {e}')
        return False

def send_key_validation_ping():
    """
    Envía un ping cifrado para validar que Central puede descifrar
    Returns: True si la clave es válida, False si es corrupta
    """
    global sock_central
    
    if not sock_central or not encryption_key:
        return True
    
    try:
        # Crear mensaje de validación
        ping_msg = {
            "type": "key_validation_ping",
            "cp_id": CP_ID,
            "timestamp": time.time()
        }
        
        # Cifrar y enviar
        encrypted_msg = encrypt_message(ping_msg)
        if not encrypted_msg:
            return False
        
        msg_str = json.dumps(encrypted_msg) + "\n"
        sock_central.sendall(msg_str.encode('utf-8'))
        
        # Esperar respuesta con timeout corto
        sock_central.settimeout(2.0)
        response = sock_central.recv(4096)
        
        if not response:
            log_message('[KEY VALIDATION] 🔴 No response - posible clave corrupta')
            return False
        
        # ✅ DEBUG: Ver qué recibimos
        response_str = response.decode('utf-8').strip()
        print(f"[KEY VALIDATION DEBUG] Respuesta recibida: {response_str[:200]}")
        
        # Parsear respuesta (solo la primera línea)
        try:
            # Si hay múltiples JSON, tomar solo el primero
            if '\n' in response_str:
                response_str = response_str.split('\n')[0]
            
            ack = json.loads(response_str)
            
            # ✅ CLAVE VÁLIDA
            if ack.get("type") == "key_validation_ack" and ack.get("status") == "ok":
                return True
            
            # 🔴 CLAVE CORRUPTA - Central reporta error
            elif ack.get("type") == "key_validation_ack" and ack.get("status") == "error":
                print("")
                print("="*70)
                print("[MONITOR] 🔴🔴🔴 CLAVE DE CIFRADO CORRUPTA 🔴🔴🔴")
                print("="*70)
                print(f"[MONITOR] ⚠️  Razón: {ack.get('reason', 'Unknown')}")
                print(f"[MONITOR] ⚠️  Error: {ack.get('error_type', 'Unknown')}")
                print("[MONITOR] 🔑 Central NO puede descifrar los mensajes")
                print("[MONITOR] ⚠️  LA COMUNICACIÓN CIFRADA ESTÁ COMPROMETIDA")
                print("="*70)
                print("")
                return False
            
            else:
                log_message(f'[KEY VALIDATION] 🔴 Respuesta inesperada: {ack}')
                return False
                
        except json.JSONDecodeError as e:
            log_message(f'[KEY VALIDATION] ❌ Error parseando respuesta: {e}')
            return False
        
    except socket.timeout:
        # Timeout es aceptable
        return True
    except (BrokenPipeError, ConnectionResetError):
        log_message('[KEY VALIDATION] 🔴 Conexión cerrada por Central')
        return False
    except Exception as e:
        log_message(f'[KEY VALIDATION] ⚠️  Error: {e}')
        return False

def check_engine_health():
    """Comprueba estado del Engine"""
    try:
        sock_engine = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_engine.settimeout(2)
        sock_engine.connect((ENGINE_HOST, ENGINE_MONITOR_PORT))
        sock_engine.sendall(b'HEALTH_CHECK')
        status = sock_engine.recv(1024).decode().strip()
        sock_engine.close()
        return status
    except Exception as e:
        return "DISCONNECTED"

def send_command_to_engine(command):
    """Envía un comando JSON al Engine"""
    try:
        sock_engine = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_engine.settimeout(5)
        sock_engine.connect((ENGINE_HOST, ENGINE_MONITOR_PORT))
        
        message = json.dumps(command) + "\n"
        sock_engine.sendall(message.encode('utf-8'))
        sock_engine.close()
        return True
    except Exception as e:
        log_message(f'[MONITOR] ❌ Error enviando comando al Engine: {e}')
        return False

def monitoring_loop():
    """Loop principal con validación de clave integrada"""
    global sock_central, registered
    
    last_status = "OK"
    last_key_validation = 0
    forced_stop = False
    
    log_message('[MONITOR] 🔐 Validación de clave: ACTIVADA (cada 5 seg)')
    
    while True:
        try:
            current_time = time.time()
            
            # Verificar conexión a Central
            if not registered or sock_central is None:
                if not connect_to_central():
                    time.sleep(5)
                    continue
                last_key_validation = current_time
            
            # Verificar comandos de Central
            try:
                sock_central.settimeout(0.1)
                data = sock_central.recv(4096)
                if data:
                    try:
                        msg = json.loads(data.decode('utf-8').strip())
                        msg_type = msg.get("type")
                        
                        if msg_type == "command":
                            action = msg.get("action")
                            if action == "stop":
                                forced_stop = True
                                log_message('[MONITOR] 🛑 Comando PARAR recibido')
                                status_msg = {
                                    "type": "status_change",
                                    "cp_id": CP_ID,
                                    "state": "PARADO"
                                }
                                send_to_central(status_msg)
                            elif action == "resume":
                                forced_stop = False
                                log_message('[MONITOR] ▶️  Comando REANUDAR recibido')
                                status_msg = {
                                    "type": "status_change",
                                    "cp_id": CP_ID,
                                    "state": "ACTIVADO"
                                }
                                send_to_central(status_msg)
                        
                        elif msg_type == "start_supply":
                            driver_id = msg.get("driver_id")
                            log_message(f'[MONITOR] 🚗 START_SUPPLY para {driver_id}')
                            engine_command = {
                                "type": "start_supply",
                                "driver_id": driver_id,
                                "cp_id": CP_ID
                            }
                            send_command_to_engine(engine_command)
                        
                        elif msg_type == "stop_supply":
                            reason = msg.get("reason", "unknown")
                            log_message(f'[MONITOR] 🛑 STOP_SUPPLY (razón: {reason})')
                            engine_command = {
                                "type": "stop_supply",
                                "reason": reason
                            }
                            send_command_to_engine(engine_command)
                    except json.JSONDecodeError:
                        pass
            except socket.timeout:
                pass
            
            if forced_stop:
                time.sleep(1)
                continue
            
            # ✅ VALIDAR CLAVE cada 5 segundos
            if encryption_key and (current_time - last_key_validation >= 5):
                key_valid = send_key_validation_ping()
                last_key_validation = current_time
                
                if not key_valid:
                    # 🔴 MANTENER ALERTA VISIBLE
                    log_message('')
                    log_message('❌'*35)
                    log_message('[MONITOR] ⚠️  CLAVE CORRUPTA - Revisar Central')
                    log_message('❌'*35)
                    log_message('')
            
            # Comprobar salud del Engine
            engine_status = check_engine_health()
            
            if engine_status != last_status:
                if engine_status == "KO":
                    fault_msg = {
                        "type": "fault",
                        "cp_id": CP_ID,
                        "msg": "Engine reportó estado KO"
                    }
                    if send_to_central(fault_msg):
                        log_message('[MONITOR] 🔴 AVERÍA notificada')
                        last_status = "KO"
                
                elif engine_status == "DISCONNECTED":
                    disconnect_msg = {
                        "type": "fault",
                        "cp_id": CP_ID,
                        "msg": "No se puede conectar con Engine"
                    }
                    if send_to_central(disconnect_msg):
                        log_message('[MONITOR] 🔴 Engine DESCONECTADO')
                        last_status = "DISCONNECTED"
                
                elif engine_status == "OK" and last_status != "OK":
                    log_message('[MONITOR] 🟢 Engine recuperado')
                    last_status = "OK"
            
        except KeyboardInterrupt:
            log_message('[MONITOR] Saliendo...')
            try:
                disconnect_msg = {
                    "type": "disconnect",
                    "cp_id": CP_ID
                }
                send_to_central(disconnect_msg)
                sock_central.close()
            except:
                pass
            break
        
        except Exception as e:
            log_message(f'[MONITOR] Error: {e}')
            registered = False
            if sock_central:
                sock_central.close()
                sock_central = None
        
        time.sleep(1)

if __name__ == '__main__':
    log_message('='*60)
    log_message(f'[MONITOR] 🚀 Iniciando Monitor para CP: {CP_ID}')
    log_message(f'[MONITOR] Engine: {ENGINE_HOST}:{ENGINE_MONITOR_PORT}')
    log_message(f'[MONITOR] Central: {CENTRAL_HOST}:{CENTRAL_PORT}')
    log_message('='*60)
    
    monitoring_loop()