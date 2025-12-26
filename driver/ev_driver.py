#!/usr/bin/env python3
"""
EV_Driver - Aplicación del conductor (MEJORADA v3)
Funciones:
- Solicita suministro en un CP específico
- Muestra telemetría en TIEMPO REAL durante el suministro
- Muestra ticket final al completar
- Puede leer peticiones desde un archivo
- Recibe updates vía Kafka de CENTRAL
- NUEVO: Detecta si CP se avería/desconecta durante suministro
"""

import os
import sys
import time
import json
import threading
import uuid
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

# Configuración
DRIVER_ID = os.getenv('DRIVER_ID', 'DRV001')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

class EVDriver:
    def __init__(self, driver_id, kafka_broker):
        self.driver_id = driver_id
        self.kafka_broker = kafka_broker
        self.producer = None
        self.consumer_updates = None
        self.consumer_telemetry = None
        self.running = True
        self.pending_requests = []
        self.current_cp = None
        self.current_price = 0.0
        self.supply_start_time = None
        self.waiting_response = False  # Flag para timeout
        self.response_received = threading.Event()  # Event para sincronizar
        
    def log(self, msg):
        """Logging con timestamp"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f'[{timestamp}] [DRIVER-{self.driver_id}] {msg}')
    
    def init_kafka(self):
        """Inicializa Kafka con reintentos"""
        max_retries = 10
        
        for attempt in range(max_retries):
            try:
                self.log(f"Conectando a Kafka en {self.kafka_broker}... (intento {attempt+1}/{max_retries})")
                
                # Producer para enviar peticiones
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_broker,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5,
                    max_block_ms=5000
                )
                
                # Consumer para recibir respuestas de CENTRAL
                self.consumer_updates = KafkaConsumer(
                    'driver_updates',
                    bootstrap_servers=self.kafka_broker,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id=f'driver_{self.driver_id}_updates',
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000
                )
                
                # Consumer para recibir telemetría de CPs
                self.consumer_telemetry = KafkaConsumer(
                    'cp_telemetry',
                    bootstrap_servers=self.kafka_broker,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    group_id=f'driver_{self.driver_id}_telemetry_{uuid.uuid4().hex[:8]}',
                    auto_offset_reset='latest',
                    enable_auto_commit=False,
                    consumer_timeout_ms=1000
                )
                
                self.log("✓ Kafka inicializado correctamente")
                self.log("✓ Escuchando: driver_updates, cp_telemetry")
                return True
                
            except Exception as e:
                if attempt < max_retries - 1:
                    self.log(f"✗ Error conectando (intento {attempt+1}): {e}")
                    time.sleep(2)
                else:
                    self.log(f"✗ Error fatal inicializando Kafka después de {max_retries} intentos: {e}")
                    return False
    
    def request_charging(self, cp_id):
        """Solicita un suministro en un CP"""
        self.log(f"Solicitando suministro en CP {cp_id}...")
        
        request = {
            "type": "request_charging",
            "driver_id": self.driver_id,
            "cp_id": cp_id,
            "timestamp": time.time()
        }
        
        try:
            # Resetear event y flag
            self.response_received.clear()
            self.waiting_response = True
            
            self.producer.send('driver_requests', request)
            self.producer.flush()
            self.log(f"✓ Petición enviada para CP {cp_id}")
            
            # ✅ Esperar respuesta con timeout de 10 segundos
            self.log("⏳ Esperando respuesta de Central...")
            received = self.response_received.wait(timeout=10)
            
            if not received:
                print("\n")
                self.log("="*60)
                self.log("⚠️  TIMEOUT: Central no respondió en 10 segundos")
                self.log("   Posibles causas:")
                self.log("   - Central está apagado")
                self.log("   - Kafka no funciona correctamente")
                self.log("   - Red desconectada")
                self.log("="*60)
                print()
                return False
            
            return True
            
        except Exception as e:
            self.log(f"✗ Error enviando petición: {e}")
            return False
        finally:
            self.waiting_response = False
    
    def consumer_updates_loop(self):
        """Loop que consume respuestas de autorización/denegación"""
        self.log("Iniciando consumer updates loop...")
        
        if not self.consumer_updates:
            self.log("✗ Consumer updates no inicializado")
            return
        
        kafka_failures = 0
        kafka_down_alerted = False  # Flag para no spamear
        
        try:
            while self.running:
                try:
                    messages = self.consumer_updates.poll(timeout_ms=1000)
                    kafka_failures = 0  # Reset si funciona
                    if kafka_down_alerted:
                        self.log("✅ Kafka reconectado - Sistema operativo")
                        kafka_down_alerted = False
                    
                except Exception as e:
                    kafka_failures += 1
                    if kafka_failures == 1:
                        self.log(f"⚠️  Error Kafka consumer_updates: {e}")
                    
                    if kafka_failures >= 5 and not kafka_down_alerted:
                        print("\n\n")
                        print("="*60)
                        print("⚠️  ALERTA: KAFKA NO RESPONDE")
                        print("="*60)
                        self.log("🔴 Sistema de notificaciones DESCONECTADO")
                        self.log("⚠️  No se pueden recibir respuestas de Central")
                        self.log("⚠️  No podrá solicitar nuevos servicios hasta reconexión")
                        print("="*60)
                        print()
                        kafka_down_alerted = True
                    continue
                
                for topic_partition, records in messages.items():
                    for record in records:
                        if not self.running:
                            break
                        
                        try:
                            data = record.value
                            driver_id = data.get('driver_id')
                            
                            if driver_id != self.driver_id:
                                continue
                            
                            payload = data.get('payload', {})
                            msg_type = payload.get('type')
                            
                            if msg_type == 'charging_authorized':
                                cp_id = payload.get('cp_id')
                                price = payload.get('price', 0.0)
                                self.current_cp = cp_id
                                self.current_price = price
                                self.supply_start_time = time.time()
                                
                                # ✅ Señalizar que llegó respuesta
                                self.response_received.set()
                                
                                print()
                                self.log("="*60)
                                self.log("✅ SUMINISTRO AUTORIZADO")
                                self.log(f"   CP: {cp_id}")
                                self.log(f"   Precio: €{price:.2f}/kWh")
                                self.log("   🔌 Conecte su vehículo al punto de recarga")
                                self.log("="*60)
                                print()
                            
                            elif msg_type == 'charging_denied':
                                cp_id = payload.get('cp_id')
                                reason = payload.get('reason', 'Desconocido')
                                
                                # ✅ Señalizar que llegó respuesta
                                self.response_received.set()
                                
                                print()
                                self.log("="*60)
                                self.log("❌ SUMINISTRO DENEGADO")
                                self.log(f"   CP: {cp_id}")
                                self.log(f"   Razón: {reason}")
                                self.log("="*60)
                                print()
                                self.current_cp = None
                            
                            elif msg_type == 'supply_cancelled':
                                cp_id = payload.get('cp_id')
                                reason = payload.get('reason', 'Unknown')
                                
                                # Solo procesar si es nuestro CP actual
                                if cp_id == self.current_cp:
                                    print("\n\n")
                                    self.log("="*60)
                                    self.log("🛑 SUMINISTRO CANCELADO POR CENTRAL")
                                    self.log(f"   Razón: {reason}")
                                    self.log("   Esperando ticket final...")
                                    self.log("="*60)
                                    print()
                                    
                                    # ✅ Resetear estado inmediatamente
                                    self.current_cp = None
                                    self.current_price = 0.0
                                    self.supply_start_time = None
                        
                        except Exception as e:
                            self.log(f"Error procesando mensaje updates: {e}")
        
        except Exception as e:
            self.log(f"Error en consumer updates loop: {e}")
            import traceback
            traceback.print_exc()
    
    def consumer_telemetry_loop(self):
        """Loop que consume telemetría en tiempo real"""
        self.log("Iniciando consumer telemetry loop...")
        
        if not self.consumer_telemetry:
            self.log("✗ Consumer telemetry no inicializado")
            return
        
        last_print = 0
        last_telemetry = None  # Watchdog - None = desactivado hasta primer mensaje
        kafka_failures = 0
        
        try:
            while self.running:
                try:
                    messages = self.consumer_telemetry.poll(timeout_ms=500)
                    kafka_failures = 0  # Reset si funciona
                    
                except Exception as e:
                    kafka_failures += 1
                    if kafka_failures == 1:
                        self.log(f"⚠️  Error Kafka consumer_telemetry: {e}")
                    if kafka_failures >= 5:
                        self.log("🔴 KAFKA NO RESPONDE - Sin telemetría")
                        
                        # Abortar suministro si estábamos recibiendo
                        if self.current_cp:
                            saved_cp = self.current_cp
                            saved_kwh = 0.0  # No sabemos cuánto llevaba
                            
                            print("\n\n")
                            print("="*60)
                            print("⚠️  ALERTA: PÉRDIDA DE CONEXIÓN CON CENTRAL")
                            print("="*60)
                            self.log(f"🔴 Suministro en {saved_cp} ABORTADO por fallo Kafka")
                            self.log("⚠️  No se puede confirmar estado final del suministro")
                            self.log("⚠️  Contacte con el operador para verificar cargo")
                            print("="*60)
                            print()
                            
                            # Intentar enviar cancelación (probablemente falle)
                            try:
                                cancel_msg = {
                                    "type": "cancel_supply",
                                    "driver_id": self.driver_id,
                                    "cp_id": saved_cp,
                                    "reason": "kafka_driver_lost"
                                }
                                self.producer.send('driver_requests', cancel_msg)
                                self.producer.flush(timeout=2)
                            except:
                                pass  # Esperado que falle
                            
                            # Resetear estado local
                            self.current_cp = None
                            self.current_price = 0.0
                            self.supply_start_time = None
                            last_telemetry = None  # Reset watchdog
                    continue
                
                if not messages:
                    # ✅ WATCHDOG: Si estamos suministrando y no recibimos telemetría en 5 seg
                    # last_telemetry será None hasta que llegue el primer mensaje
                    if self.current_cp and self.supply_start_time and last_telemetry is not None:
                        silence_duration = time.time() - last_telemetry
                        if silence_duration > 5:  # 5 segundos
                            print("\n\n")
                            print("="*60)
                            print("⚠️  ALERTA: SIN TELEMETRÍA > 5 SEGUNDOS")
                            print("="*60)
                            self.log(f"🔴 Suministro en {self.current_cp} ABORTADO (sin datos)")
                            self.log("⚠️  Central/CP no responde - Verificar conexión")
                            print("="*60)
                            print()
                            
                            self.current_cp = None
                            self.current_price = 0.0
                            self.supply_start_time = None
                            last_telemetry = None  # Reset watchdog
                    
                    continue
                
                for topic_partition, records in messages.items():
                    for record in records:
                        if not self.running:
                            break
                        
                        try:
                            data = record.value
                            msg_type = data.get('type')
                            cp_id = data.get('cp_id')
                            driver_id = data.get('driver_id')
                            
                            # Solo procesar mensajes para nosotros
                            if driver_id and driver_id != self.driver_id:
                                continue
                            
                            # Para telemetría y supply_start: solo si es el CP actual
                            if msg_type in ['telemetry', 'supply_start'] and cp_id != self.current_cp:
                                continue
                            
                            # supply_end (ticket): SIEMPRE procesarlo si es para nosotros
                            
                            if msg_type == 'supply_start':
                                last_telemetry = time.time()  # Reset watchdog
                                print()
                                self.log("🔋 SUMINISTRO INICIADO - Esperando telemetría...")
                                print()
                            
                            elif msg_type == 'telemetry':
                                last_telemetry = time.time()  # Reset watchdog
                                now = time.time()
                                if now - last_print >= 2.0:
                                    is_supplying = data.get('is_supplying', False)
                                    if is_supplying:
                                        kw = data.get('consumption_kw', 0.0)
                                        kwh = data.get('total_kwh', 0.0)
                                        price = data.get('current_price', self.current_price)
                                        euros = kwh * price
                                        
                                        if self.supply_start_time:
                                            elapsed = int(now - self.supply_start_time)
                                            mins = elapsed // 60
                                            secs = elapsed % 60
                                            time_str = f"{mins:02d}:{secs:02d}"
                                        else:
                                            time_str = "00:00"
                                        
                                        print(f"\r⚡ {kw:5.1f} kW | 🔋 {kwh:6.3f} kWh | 💶 €{euros:6.2f} | ⏱️  {time_str}", end='', flush=True)
                                        last_print = now
                            
                            elif msg_type == 'supply_end':
                                # TICKET FINAL
                                last_telemetry = time.time()  # Reset watchdog
                                
                                total_kwh = data.get('total_kwh', 0.0)
                                total_euros = data.get('total_euros', 0.0)
                                reason = data.get('reason', 'completed')
                                
                                if self.supply_start_time:
                                    duration = int(time.time() - self.supply_start_time)
                                    mins = duration // 60
                                    secs = duration % 60
                                else:
                                    mins, secs = 0, 0
                                
                                # Imprimir ticket
                                print("\n\n")
                                print("="*60)
                                print("🧾 TICKET DE RECARGA")
                                print("="*60)
                                print(f"Conductor:        {self.driver_id}")
                                print(f"Punto de recarga: {cp_id}")
                                print(f"Fecha:            {time.strftime('%Y-%m-%d %H:%M:%S')}")
                                print("-"*60)
                                
                                if reason == 'completed':
                                    print("Estado:           ✅ COMPLETADO")
                                elif reason == 'driver_disconnected':
                                    print("Estado:           🛑 CANCELADO (Desconexión)")
                                elif reason == 'fault':
                                    print("Estado:           ⚠️  INTERRUMPIDO (Avería CP)")
                                elif reason == 'admin':
                                    print("Estado:           🛑 CANCELADO (Administrativo)")
                                elif reason == 'cp_disconnected':
                                    print("Estado:           🔴 INTERRUMPIDO (CP Desconectado)")
                                elif reason == 'engine_shutdown':
                                    print("Estado:           🔴 INTERRUMPIDO (Engine desconectado)")
                                elif reason == 'kafka_disconnected':
                                    print("Estado:           🔴 INTERRUMPIDO (Central desconectado)")
                                else:
                                    print(f"Estado:           ⚠️  FINALIZADO ({reason})")
                                
                                print("-"*60)
                                print(f"⚡ Energía:         {total_kwh:.3f} kWh")
                                print(f"💶 Importe:         €{total_euros:.2f}")
                                print(f"⏱️  Duración:        {mins:02d}:{secs:02d}")
                                print("="*60)
                                print()
                                
                                # Resetear estado
                                self.current_cp = None
                                self.current_price = 0.0
                                self.supply_start_time = None
                        
                        except Exception as e:
                            self.log(f"Error procesando telemetría: {e}")
        
        except Exception as e:
            self.log(f"Error en consumer telemetry loop: {e}")
    
    def load_requests_from_file(self, filename):
        """Carga peticiones desde un archivo"""
        try:
            with open(filename, 'r') as f:
                lines = [line.strip() for line in f if line.strip()]
                self.pending_requests = lines
                self.log(f"✓ Cargadas {len(lines)} peticiones desde {filename}")
                return True
        except Exception as e:
            self.log(f"✗ Error cargando archivo: {e}")
            return False
    
    def process_requests_from_file(self):
        """Procesa las peticiones del archivo una a una"""
        self.log(f"Procesando {len(self.pending_requests)} peticiones...")
        
        for cp_id in self.pending_requests:
            if not self.running:
                break
            
            self.log(f"--- Petición para CP: {cp_id} ---")
            self.request_charging(cp_id)
            
            # Esperar 4 segundos antes de la siguiente
            for i in range(4, 0, -1):
                if not self.running:
                    break
                time.sleep(1)
        
        self.log("Todas las peticiones procesadas")
    
    def interactive_mode(self):
        """Modo interactivo por teclado"""
        print()
        self.log("="*60)
        self.log("MODO INTERACTIVO:")
        self.log("  r <CP_ID> = Solicitar suministro en CP")
        self.log("  f <archivo> = Cargar y procesar peticiones desde archivo")
        self.log("  s = Ver estado actual")
        self.log("  q = Salir")
        self.log("="*60)
        print()
        
        while self.running:
            try:
                if self.current_cp:
                    prompt = f"{self.driver_id} [🔋 {self.current_cp}]> "
                else:
                    prompt = f"{self.driver_id}> "
                
                cmd = input(prompt).strip()
                
                if not cmd:
                    continue
                
                parts = cmd.split()
                action = parts[0].lower()
                
                if action == 'q':
                    self.log("Saliendo...")
                    self.running = False
                    break
                
                elif action == 'r' and len(parts) >= 2:
                    cp_id = parts[1]
                    self.request_charging(cp_id)
                
                elif action == 'f' and len(parts) >= 2:
                    filename = parts[1]
                    if self.load_requests_from_file(filename):
                        t = threading.Thread(target=self.process_requests_from_file, daemon=True)
                        t.start()
                
                elif action == 's':
                    if self.current_cp:
                        self.log(f"📍 Suministrando en: {self.current_cp}")
                        self.log(f"💶 Precio: €{self.current_price:.2f}/kWh")
                    else:
                        self.log("Sin suministro activo")
                
                else:
                    self.log("Comando no reconocido")
                
            except EOFError:
                self.log("\nEOF detectado, saliendo...")
                self.running = False
                break
            except KeyboardInterrupt:
                self.log("\nInterrupción detectada, saliendo...")
                self.running = False
                break
            except Exception as e:
                self.log(f"Error en interactive mode: {e}")
    
    def run(self):
        """Ejecuta la aplicación"""
        if not self.init_kafka():
            self.log("Error inicializando Kafka, saliendo...")
            return
        
        # Esperar a que Kafka esté completamente listo
        self.log("Esperando estabilización de Kafka...")
        time.sleep(3)  # ✅ Aumentado a 3 segundos
        
        # Verificar que los consumers están listos
        if not self.consumer_updates or not self.consumer_telemetry:
            self.log("✗ Consumers no inicializados correctamente")
            return
        
        # Iniciar threads
        t1 = threading.Thread(target=self.consumer_updates_loop, daemon=True)
        t1.start()
        
        t2 = threading.Thread(target=self.consumer_telemetry_loop, daemon=True)
        t2.start()
        
        # Dar tiempo a que se inicien los threads
        self.log("Iniciando threads de consumo...")
        time.sleep(3)  # ✅ Aumentado a 3 segundos
        
        self.log("✅ Sistema listo")
        
        # Modo interactivo
        self.interactive_mode()
        
        # Cleanup
        self.log("Cerrando...")
        if self.consumer_updates:
            self.consumer_updates.close()
        if self.consumer_telemetry:
            self.consumer_telemetry.close()
        if self.producer:
            self.producer.close()

if __name__ == "__main__":
    driver = EVDriver(DRIVER_ID, KAFKA_BROKER)
    driver.run()