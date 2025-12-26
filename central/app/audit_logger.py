"""
AuditLogger - Sistema de auditoría de eventos
"""
import os
import json
from datetime import datetime

class AuditLogger:
    def __init__(self, log_file="/central/audit.log"):
        self.log_file = log_file
        # Crear archivo si no existe
        if not os.path.exists(self.log_file):
            os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
            open(self.log_file, 'a').close()
    
    def log(self, event_type, source_ip, source_port, description, **kwargs):
        """
        Registra un evento en el log de auditoría
        
        Args:
            event_type: Tipo de evento (AUTH_SUCCESS, AUTH_FAILURE, etc.)
            source_ip: IP de origen
            source_port: Puerto de origen
            description: Descripción del evento
            **kwargs: Datos adicionales en formato clave=valor
        """
        timestamp = datetime.utcnow().isoformat()
        
        # Construir entrada de log
        log_entry = f"[{timestamp}] [{source_ip}:{source_port}] [{event_type}] - {description}"
        
        # Añadir datos adicionales si existen
        if kwargs:
            log_entry += f" | {json.dumps(kwargs)}"
        
        # Escribir en archivo
        try:
            with open(self.log_file, 'a') as f:
                f.write(log_entry + '\n')
        except Exception as e:
            print(f"[AUDIT] ❌ Error escribiendo log: {e}")
        
        # También imprimir en consola
        print(f"[AUDIT] {log_entry}")
    
    def log_auth_success(self, cp_id, source_ip, source_port):
        """Registra autenticación exitosa"""
        self.log(
            "AUTH_SUCCESS",
            source_ip,
            source_port,
            f"CP {cp_id} autenticado exitosamente",
            cp_id=cp_id,
            status="success"
        )
    
    def log_auth_failure(self, cp_id, source_ip, source_port, reason):
        """Registra fallo de autenticación"""
        self.log(
            "AUTH_FAILURE",
            source_ip,
            source_port,
            f"Intento de autenticación fallido para {cp_id}: {reason}",
            cp_id=cp_id,
            reason=reason,
            status="denied"
        )
    
    def log_key_generated(self, cp_id, source_ip, source_port):
        """Registra generación de clave"""
        self.log(
            "KEY_GENERATED",
            source_ip,
            source_port,
            f"Nueva clave de cifrado generada para {cp_id}",
            cp_id=cp_id
        )
    
    def log_key_revoked(self, cp_id, source_ip, source_port):
        """Registra revocación de clave"""
        self.log(
            "KEY_REVOKED",
            source_ip,
            source_port,
            f"Clave de cifrado revocada para {cp_id}",
            cp_id=cp_id
        )
    
    def log_decryption_failed(self, cp_id, source_ip, source_port):
        """Registra fallo de descifrado (clave corrupta)"""
        self.log(
            "DECRYPTION_FAILED",
            source_ip,
            source_port,
            f"Error descifrando mensaje de {cp_id} - Clave corrupta o inválida",
            cp_id=cp_id,
            status="error"
        )
        self.log(
            "KEY_REVOKED",
            source_ip,
            source_port,
            f"Clave de cifrado revocada para {cp_id}",
            cp_id=cp_id
        )
    
    def log_security_reset(self, keys_count, source_ip, source_port):
        """Registra reset de seguridad"""
        self.log(
            "SECURITY_RESET",
            source_ip,
            source_port,
            f"Reset de seguridad: {keys_count} claves revocadas",
            keys_revoked=keys_count
        )
    
    def log_command(self, cp_id, command, status, source_ip, source_port):
        """Registra comando enviado a CP"""
        self.log(
            "COMMAND_SENT",
            source_ip,
            source_port,
            f"Comando '{command}' enviado a {cp_id}",
            cp_id=cp_id,
            command=command,
            status=status
        )
    
    def log_cp_fault(self, cp_id, fault_reason, source_ip, source_port):
        """Registra avería de CP"""
        self.log(
            "CP_FAULT",
            source_ip,
            source_port,
            f"Avería detectada en {cp_id}: {fault_reason}",
            cp_id=cp_id,
            reason=fault_reason
        )
    
    def log_cp_recovery(self, cp_id, source_ip, source_port):
        """Registra recuperación de CP"""
        self.log(
            "CP_RECOVERY",
            source_ip,
            source_port,
            f"CP {cp_id} recuperado de avería",
            cp_id=cp_id
        )
    
    def log_encryption_test(self, cp_id, status, source_ip, source_port):
        """Registra test de cifrado"""
        self.log(
            "ENCRYPTION_TEST",
            source_ip,
            source_port,
            f"Test de cifrado para {cp_id}: {status}",
            cp_id=cp_id,
            status=status
        )
    
    def log_decryption_failed(self, cp_id, source_ip, source_port):
        """Registra fallo de descifrado"""
        self.log(
            "DECRYPTION_FAILED",
            source_ip,
            source_port,
            f"Error descifrando mensaje de {cp_id} - posible clave corrupta",
            cp_id=cp_id,
            severity="high"
        )
    
    def get_recent_logs(self, count=100):
        """Obtiene los últimos N logs"""
        try:
            with open(self.log_file, 'r') as f:
                lines = f.readlines()
                return [line.strip() for line in lines[-count:]]
        except Exception as e:
            print(f"[AUDIT] ❌ Error leyendo logs: {e}")
            return []
    
    def search_logs(self, keyword, count=100):
        """Busca en los logs por palabra clave"""
        try:
            with open(self.log_file, 'r') as f:
                lines = f.readlines()
                matching = [line.strip() for line in lines if keyword.lower() in line.lower()]
                return matching[-count:]
        except Exception as e:
            print(f"[AUDIT] ❌ Error buscando en logs: {e}")
            return []