import tkinter as tk
from tkinter import ttk
from tkinter import font, filedialog, messagebox
import queue
import os

COLOR_VERDE = "#4CAF50"
COLOR_ROJO = "#F44336"
COLOR_GRIS = "#9E9E9E"
COLOR_FONDO = "#333333"
COLOR_TEXTO = "#FFFFFF"

class DriverApp(tk.Tk):
    
    def __init__(self, gui_queue):
        super().__init__()
        self.title("EV Driver Application")
        self.geometry("900x600")
        self.configure(bg="#212121", padx=10, pady=10)
        
        self.gui_queue = gui_queue
        self.backend_controller = None
        
        self.cp_status_map = {}
        
        self._create_widgets()
        
        self.after(100, self._process_queue)

    def set_controller(self, controller):
        """Almacena una referencia al objeto del backend (EV_Driver)."""
        self.backend_controller = controller

    def _create_widgets(self):
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=3)
        self.rowconfigure(0, weight=0) 
        self.rowconfigure(1, weight=1) 
        self.rowconfigure(2, weight=1) 

        self._create_connect_frame().grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        
        self._create_request_frame().grid(row=1, column=0, sticky="nsew", padx=(0, 5))
        
        self._create_cp_list_frame().grid(row=1, column=1, sticky="nsew", padx=(5, 0))
        
        self._create_message_log_frame().grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(10, 0))

    def _create_connect_frame(self):
        frame = tk.Frame(self, bg=COLOR_FONDO, pady=5)
        
        tk.Label(frame, text="Driver ID:", bg=COLOR_FONDO, fg=COLOR_TEXTO).pack(side="left", padx=(5, 2))
        self.entry_driver_id = tk.Entry(frame, width=15)
        self.entry_driver_id.pack(side="left", padx=2)
        
        tk.Label(frame, text="Kafka Broker:", bg=COLOR_FONDO, fg=COLOR_TEXTO).pack(side="left", padx=(10, 2))
        self.entry_kafka_broker = tk.Entry(frame, width=20)
        self.entry_kafka_broker.insert(0, "localhost:9092")
        self.entry_kafka_broker.pack(side="left", padx=2)
        
        self.btn_connect = tk.Button(frame, text="Conectar", command=self._on_connect, bg=COLOR_VERDE, fg=COLOR_TEXTO)
        self.btn_connect.pack(side="left", padx=10)
        
        return frame
        
    def _create_request_frame(self):
        frame = tk.LabelFrame(self, text=" Realizar Petición ", fg=COLOR_TEXTO, bg=COLOR_FONDO, padx=10, pady=10)
        
        tk.Label(frame, text="Estado de Carga Actual:", fg=COLOR_TEXTO, bg=COLOR_FONDO, font=("Arial", 12, "bold")).pack(pady=5)
        self.lbl_current_cp = tk.Label(frame, text="CP: ---", fg=COLOR_TEXTO, bg=COLOR_FONDO, font=("Arial", 10))
        self.lbl_current_cp.pack()
        self.lbl_kwh = tk.Label(frame, text="0.0 kWh", fg=COLOR_VERDE, bg=COLOR_FONDO, font=("Arial", 14, "bold"))
        self.lbl_kwh.pack(pady=5)
        self.lbl_euros = tk.Label(frame, text="0.00 €", fg=COLOR_VERDE, bg=COLOR_FONDO, font=("Arial", 14, "bold"))
        self.lbl_euros.pack()
        
        tk.Label(frame, text="ID del CP Manual:", fg=COLOR_TEXTO, bg=COLOR_FONDO).pack(pady=(15, 2))
        self.entry_manual_cp = tk.Entry(frame, width=15)
        self.entry_manual_cp.pack(pady=2)
        self.btn_request_manual = tk.Button(frame, text="Solicitar Carga Manual", command=self._on_request_charge, state="disabled")
        self.btn_request_manual.pack(pady=5)
        
        self.btn_request_file = tk.Button(frame, text="Cargar Servicios desde Archivo", command=self._on_load_file, state="disabled")
        self.btn_request_file.pack(pady=(15, 5))
        
        return frame

    def _create_cp_list_frame(self):
        frame = tk.LabelFrame(self, text=" Puntos de Carga Disponibles ", fg=COLOR_TEXTO, bg=COLOR_FONDO, padx=10, pady=10)
        frame.pack_propagate(False)
        
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("Treeview", background="#555555", foreground="white", fieldbackground="#555555", rowheight=25)
        style.map('Treeview', background=[('selected', '#0D47A1')])
        style.configure("Treeview.Heading", background="#444444", foreground="white", font=("Arial", 10, "bold"))

        style.configure("green.Treeview", foreground=COLOR_VERDE)
        style.configure("red.Treeview", foreground=COLOR_ROJO)
        style.configure("grey.Treeview", foreground=COLOR_GRIS)
        
        cols = ('CP ID', 'Ubicación', 'Precio', 'Estado')
        self.cp_tree = ttk.Treeview(frame, columns=cols, show='headings')
        
        for col in cols:
            self.cp_tree.heading(col, text=col)
            self.cp_tree.column(col, width=120, anchor="center")
            
        self.cp_tree.pack(fill="both", expand=True)
        return frame

    def _create_message_log_frame(self):
        frame = tk.LabelFrame(self, text=" Log de Mensajes ", fg=COLOR_TEXTO, bg=COLOR_FONDO, padx=10, pady=10)
        frame.pack_propagate(False)
        
        self.log_listbox = tk.Listbox(frame, bg="#555555", fg="white", selectbackground="#0D47A1", font=("Consolas", 10))
        self.log_listbox.pack(fill="both", expand=True)
        return frame


    def _on_connect(self):
        driver_id = self.entry_driver_id.get()
        broker = self.entry_kafka_broker.get()
        
        if not driver_id or not broker:
            messagebox.showerror("Error", "Debe introducir un ID de Conductor y un Broker.")
            return
            
        if self.backend_controller:
            self.backend_controller.connect(driver_id, broker)
            self.btn_connect.config(state="disabled", text="Conectado")
            self.entry_driver_id.config(state="disabled")
            self.entry_kafka_broker.config(state="disabled")
            self.btn_request_manual.config(state="normal")
            self.btn_request_file.config(state="normal")
            self.title(f"EV Driver Application - {driver_id}")
            self._add_log_message(f"Conectando como '{driver_id}' a '{broker}'...")
            
    def _on_request_charge(self):
        cp_id = self.entry_manual_cp.get()
        if not cp_id:
            messagebox.showerror("Error", "Debe introducir un ID de CP.")
            return
            
        if self.backend_controller:
            self.backend_controller.request_manual_service(cp_id)
            self.entry_manual_cp.delete(0, "end")

    def _on_load_file(self):
        
            try:
                # __file__ es la ruta de este script (driver_gui.py)
                script_dir = os.path.dirname(os.path.abspath(__file__))
                file_path = os.path.join(script_dir, "services.txt")
            
                if not os.path.exists(file_path):
                    messagebox.showerror("Error", f"No se encontró el archivo 'services.txt' en:\n{file_path}")
                    return
            
                if self.backend_controller:
                    self.backend_controller.start_file_services(file_path)
                else:
                    self._add_log_message("ERROR: Backend no conectado.")
                
            except Exception as e:
                messagebox.showerror("Error", f"Fallo al cargar services.txt: {e}")
        
    def _add_log_message(self, message):
        self.log_listbox.insert("end", message)
        self.log_listbox.see("end") 
        
    def _update_charge_display(self, data):
        cp_id = data.get('cp_id', '---')
        kwh = data.get('kwh', data.get('kwh_actual', 0.0))
        euros = data.get('euros', data.get('euros_actual', 0.0))
        
        self.lbl_current_cp.config(text=f"CP: {cp_id}")
        self.lbl_kwh.config(text=f"{kwh:.2f} kWh")
        self.lbl_euros.config(text=f"{euros:.2f} €")
        
    def _reset_charge_display(self):
        self.lbl_current_cp.config(text="CP: ---")
        self.lbl_kwh.config(text="0.0 kWh")
        self.lbl_euros.config(text="0.00 €")
        
    def _update_cp_list(self, data):
        cp_id = data.get('cp_id')
        if not cp_id:
            return
            
        loc = data.get('location', 'N/A')
        price = data.get('price_kwh', 'N/A')
        status = data.get('status', 'N/A')
        
        tag = ''
        if status == 'ACTIVADO':
            tag = 'green'
        elif status in ('AVERIADO', 'PARADO'):
            tag = 'red'
        elif status == 'DESCONECTADO':
            tag = 'grey'
        
        values = (cp_id, loc, price, status)
        
        if cp_id in self.cp_status_map:
            self.cp_tree.item(self.cp_status_map[cp_id], values=values, tags=(tag,))
        else:
            item_id = self.cp_tree.insert("", "end", values=values, tags=(tag,))
            self.cp_status_map[cp_id] = item_id

    
    def _process_queue(self):
        try:
            while True:
                message = self.gui_queue.get_nowait()
                msg_type, data = message
                
                if msg_type == "ADD_MESSAGE":
                    self._add_log_message(data)
                
                elif msg_type == "UPDATE_CHARGE":
                    self._update_charge_display(data)
                    
                elif msg_type == "RESET_CHARGE":
                    self._reset_charge_display()
                    
                elif msg_type == "UPDATE_CP_LIST":
                    self._update_cp_list(data)

        except queue.Empty:
            pass 
        finally:
            self.after(100, self._process_queue)