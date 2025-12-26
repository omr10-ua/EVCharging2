"""
CryptoManager - Gestión de claves de cifrado para CPs
"""
import os
import json
from cryptography.fernet import Fernet
from datetime import datetime

class CryptoManager:
    def __init__(self, keys_file="/central/app/crypto_keys.json"):
        self.keys_file = keys_file
        self.keys = {}
        self._load_keys()
    
    def _load_keys(self):
        """Carga claves existentes desde archivo"""
        if os.path.exists(self.keys_file):
            try:
                with open(self.keys_file, 'r') as f:
                    self.keys = json.load(f)
                print(f"[CRYPTO] ✅ Cargadas {len(self.keys)} claves desde {self.keys_file}")
            except Exception as e:
                print(f"[CRYPTO] ⚠️  Error cargando claves: {e}")
                self.keys = {}
        else:
            print(f"[CRYPTO] ℹ️  No existe {self.keys_file}, creando nuevo...")
            self.keys = {}
            self._save_keys()
    
    def _save_keys(self):
        """Guarda claves en archivo"""
        try:
            # Crear directorio si no existe
            os.makedirs(os.path.dirname(self.keys_file), exist_ok=True)
            
            with open(self.keys_file, 'w') as f:
                json.dump(self.keys, f, indent=2)
        except Exception as e:
            print(f"[CRYPTO] ❌ Error guardando claves: {e}")
    
    def generate_key_for_cp(self, cp_id):
        """Genera una nueva clave Fernet para un CP"""
        key = Fernet.generate_key().decode('utf-8')
        self.keys[cp_id] = {
            "key": key,
            "created_at": datetime.utcnow().isoformat()
        }
        self._save_keys()
        print(f"[CRYPTO] ✅ Clave generada para {cp_id}")
        return key
    
    def get_key_for_cp(self, cp_id):
        """Obtiene la clave de un CP (recarga desde archivo)"""
        if os.path.exists(self.keys_file):
            self._load_keys()  # Recargar solo si existe
        
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
            del self.keys[cp_id]
            self._save_keys()
            print(f"[CRYPTO] 🔴 Clave revocada para {cp_id}")
            return True
        return False
    
    def revoke_all_keys(self):
        """Revoca TODAS las claves (reset de seguridad)"""
        count = len(self.keys)
        self.keys = {}
        self._save_keys()
        print(f"[CRYPTO] 🔴 RESET: {count} claves revocadas")
        return count