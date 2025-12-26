"""
AuditLogger - Sistema de auditoría para Central
Versión que guarda en /central/app/ (directorio YA montado en docker-compose)
"""
import os
from datetime import datetime
import json

class AuditLogger:
    def __init__(self, log_file="/central/app/audit.log"):
        self.log_file = log_file
        print(f"[AUDIT] 📝 Inicializando AuditLogger...")
        print(f"[AUDIT] 📁 Archivo de log: {log_file}")
        
        # Crear archivo si no existe
        if not os.path.exists(log_file):
            try:
                with open(log_file, 'w') as f:
                    f.write(f"# EVCharging Audit Log - Iniciado {datetime.utcnow().isoformat()}\n")
                    f.flush()
                    os.fsync(f.fileno())
                print(f"[AUDIT] ✅ Archivo de auditoría creado")
            except Exception as e:
                print(f"[AUDIT] ❌ Error creando archivo: {e}")
        
        print(f"[AUDIT] ✅ AuditLogger listo")
    
    def log_event(self, event_type, source_ip, description, metadata=None):
        """
        Registra un evento de auditoría
        
        event_type: Tipo de evento (AUTH_SUCCESS, AUTH_FAIL, KEY_GENERATED, etc.)
        source_ip: IP origen del evento
        description: Descripción legible
        metadata: Dict con datos adicionales (opcional)
        """
        try:
            timestamp = datetime.utcnow().isoformat()
            
            # Formatear metadata como JSON compacto
            meta_str = ""
            if metadata:
                meta_str = " | " + json.dumps(metadata, ensure_ascii=False)
            
            # Formato: [TIMESTAMP] [IP:PORT] [EVENT_TYPE] - DESCRIPTION | METADATA
            log_line = f"[{timestamp}] [{source_ip}] [{event_type}] - {description}{meta_str}\n"
            
            # Escribir con flush forzado
            with open(self.log_file, 'a') as f:
                f.write(log_line)
                f.flush()
                os.fsync(f.fileno())
                
        except Exception as e:
            print(f"[AUDIT] ❌ Error escribiendo log: {e}")
    
    def log_auth_request(self, cp_id, source_ip):
        """Registra solicitud de autenticación"""
        self.log_event(
            "AUTH_REQUEST",
            source_ip,
            f"CP {cp_id} solicitando autenticación",
            {"username": cp_id}
        )
    
    def log_auth_success(self, cp_id, source_ip):
        """Registra autenticación exitosa"""
        self.log_event(
            "AUTH_SUCCESS",
            source_ip,
            f"CP {cp_id} autenticado exitosamente",
            {"username": cp_id}
        )
    
    def log_auth_failure(self, cp_id, source_ip, reason):
        """Registra autenticación fallida"""
        self.log_event(
            "AUTH_FAILURE",
            source_ip,
            f"Autenticación fallida para {cp_id}: {reason}",
            {"username": cp_id, "reason": reason}
        )
    
    def log_key_generated(self, cp_id, source_ip):
        """Registra generación de nueva clave"""
        self.log_event(
            "KEY_GENERATED",
            source_ip,
            f"Nueva clave generada para {cp_id}",
            {}
        )
    
    def log_key_revoked(self, cp_id, admin_ip, reason=None):
        """Registra revocación de clave"""
        metadata = {}
        if reason:
            metadata["reason"] = reason
        
        self.log_event(
            "KEY_REVOKED",
            admin_ip,
            f"Clave revocada para {cp_id}",
            metadata
        )
    
    def log_command_sent(self, cp_id, command, admin_ip):
        """Registra comando administrativo enviado"""
        self.log_event(
            "ADMIN_COMMAND",
            admin_ip,
            f"Comando '{command}' enviado a {cp_id}",
            {"cp_id": cp_id, "command": command}
        )
    
    def log_cp_fault(self, cp_id, source_ip, fault_msg):
        """Registra fallo/avería de CP"""
        self.log_event(
            "CP_FAULT",
            source_ip,
            f"Avería reportada en {cp_id}: {fault_msg}",
            {"cp_id": cp_id, "fault": fault_msg}
        )
    
    def log_service_start(self, cp_id, driver_id, source_ip):
        """Registra inicio de servicio"""
        self.log_event(
            "SERVICE_START",
            source_ip,
            f"Servicio iniciado: {driver_id} en {cp_id}",
            {"cp_id": cp_id, "driver_id": driver_id}
        )
    
    def log_service_end(self, cp_id, driver_id, kwh, euros, source_ip):
        """Registra finalización de servicio"""
        self.log_event(
            "SERVICE_END",
            source_ip,
            f"Servicio finalizado: {driver_id} en {cp_id}",
            {
                "cp_id": cp_id,
                "driver_id": driver_id,
                "kwh": round(kwh, 3),
                "euros": round(euros, 2)
            }
        )
    
    def log_unauthorized_access(self, source_ip, reason):
        """Registra intento de acceso no autorizado"""
        self.log_event(
            "UNAUTHORIZED_ACCESS",
            source_ip,
            f"Intento de acceso no autorizado: {reason}",
            {"reason": reason}
        )
    
    def log_system_event(self, event, source="SYSTEM"):
        """Registra evento general del sistema"""
        self.log_event(
            "SYSTEM_EVENT",
            source,
            event,
            {}
        )