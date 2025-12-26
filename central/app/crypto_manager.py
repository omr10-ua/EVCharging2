"""
CryptoManager - Gestión de claves de cifrado para CPs
Versión que guarda en /central/app/ (directorio YA montado en docker-compose)
"""
import os
import json
from cryptography.fernet import Fernet
from datetime import datetime

class CryptoManager:
    def __init__(self, keys_file="/central/app/crypto_keys.json"):
        self.keys_file = keys_file
        self.keys = {}
        print(f"[CRYPTO] 🔐 Inicializando CryptoManager...")
        print(f"[CRYPTO] 📁 Archivo de claves: {keys_file}")
        self._load_keys()
    
    def _load_keys(self):
        """Carga claves existentes desde archivo"""
        if os.path.exists(self.keys_file):
            try:
                with open(self.keys_file, 'r') as f:
                    content = f.read().strip()
                    
                    if not content or content == '{}' or content == '':
                        print(f"[CRYPTO] ⚠️  Archivo vacío, inicializando...")
                        self.keys = {}
                        self._save_keys()
                        return
                    
                    self.keys = json.loads(content)
                    print(f"[CRYPTO] ✅ Cargadas {len(self.keys)} claves desde {self.keys_file}")
            except Exception as e:
                print(f"[CRYPTO] ⚠️  Error cargando: {e}")
                self.keys = {}
                self._save_keys()
        else:
            print(f"[CRYPTO] ℹ️  Archivo no existe, creando...")
            self.keys = {}
            self._save_keys()
    
    def _save_keys(self):
        """Guarda claves con flush forzado"""
        try:
            # Escribir con flush
            with open(self.keys_file, 'w') as f:
                json.dump(self.keys, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            
            # Verificar
            if os.path.exists(self.keys_file):
                size = os.path.getsize(self.keys_file)
                print(f"[CRYPTO] 💾 Guardadas {len(self.keys)} claves en {self.keys_file} ({size} bytes)")
            else:
                print(f"[CRYPTO] ❌ Archivo no existe después de escribir")
                
        except Exception as e:
            print(f"[CRYPTO] ❌ Error guardando: {e}")
            import traceback
            traceback.print_exc()
    
    def generate_key_for_cp(self, cp_id):
        """Genera una nueva clave Fernet para un CP"""
        try:
            key = Fernet.generate_key().decode('utf-8')
            
            self.keys[cp_id] = {
                "key": key,
                "created_at": datetime.utcnow().isoformat(),
                "algorithm": "Fernet (AES-256)",
                "status": "active"
            }
            
            # Guardar inmediatamente
            self._save_keys()
            
            print(f"[CRYPTO] ✅ Clave generada y guardada para {cp_id}")
            return key
            
        except Exception as e:
            print(f"[CRYPTO] ❌ Error generando clave: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_key_for_cp(self, cp_id):
        """Obtiene la clave de un CP"""
        if cp_id in self.keys:
            key_data = self.keys[cp_id]
            if isinstance(key_data, dict):
                return key_data.get("key")
            return key_data
        return None
    
    def has_key(self, cp_id):
        """Verifica si un CP tiene clave"""
        return cp_id in self.keys
    
    def revoke_key(self, cp_id):
        """Revoca la clave de un CP"""
        if cp_id in self.keys:
            if isinstance(self.keys[cp_id], dict):
                self.keys[cp_id]["status"] = "revoked"
                self.keys[cp_id]["revoked_at"] = datetime.utcnow().isoformat()
            self._save_keys()
            print(f"[CRYPTO] 🔴 Clave revocada para {cp_id}")
            return True
        return False
    
    def delete_key(self, cp_id):
        """Elimina completamente la clave de un CP"""
        if cp_id in self.keys:
            del self.keys[cp_id]
            self._save_keys()
            print(f"[CRYPTO] 🗑️  Clave eliminada para {cp_id}")
            return True
        return False
    
    def revoke_all_keys(self):
        """Revoca TODAS las claves (reset de seguridad)"""
        count = len(self.keys)
        self.keys = {}
        self._save_keys()
        print(f"[CRYPTO] 🔴 RESET: {count} claves revocadas")
        return count
    
    def get_all_keys(self):
        """Obtiene todas las claves (para debug)"""
        return self.keys.copy()