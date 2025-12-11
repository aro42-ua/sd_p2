"""
Módulo central_gui.py
Contiene la clase CentralApp (la ventana principal de Tkinter) y el 
widget CPPanel (cada panel individual de la rejilla).
"""

import tkinter as tk
from tkinter import ttk
from tkinter import font
import queue 

COLOR_VERDE = "#4CAF50"      # Activado / Suministrando
COLOR_NARANJA = "#FF9800"   # Parado (Out of Order)
COLOR_ROJO = "#F44336"      # Averiado
COLOR_GRIS = "#9E9E9E"      # Desconectado
COLOR_FONDO_REJILLA = "#333333" 
COLOR_TEXTO = "#FFFFFF"        

class CPPanel(tk.Frame):
   
    def __init__(self, parent, cp_id, location, price):
        super().__init__(parent, borderwidth=2, relief="solid", bg=COLOR_GRIS, padx=5, pady=5)
        
        self.cp_id = cp_id
        
        self.lbl_id = tk.Label(self, text=cp_id, bg=COLOR_GRIS, fg=COLOR_TEXTO, font=("Arial", 12, "bold"))
        self.lbl_id.pack(fill="x")
        
        self.lbl_location = tk.Label(self, text=location, bg=COLOR_GRIS, fg=COLOR_TEXTO, font=("Arial", 10))
        self.lbl_location.pack(fill="x")
        
        self.lbl_price = tk.Label(self, text=price, bg=COLOR_GRIS, fg=COLOR_TEXTO, font=("Arial", 10))
        self.lbl_price.pack(fill="x")
        
        self.lbl_status = tk.Label(self, text="", bg=COLOR_GRIS, fg=COLOR_TEXTO, font=("Arial", 10, "italic"))
        self.lbl_status.pack(fill="x", pady=(5,0))
        
        self.widgets = [self, self.lbl_id, self.lbl_location, self.lbl_price, self.lbl_status]
        
        # Estado inicial Desconectado
        self.update_state("DESCONECTADO")

    def _set_colors(self, color):
        for w in self.widgets:
            w.configure(bg=color)

    def update_state(self, state, data=None):
        try:
            if state == "ACTIVADO":
                self._set_colors(COLOR_VERDE)
                self.lbl_status.configure(text="")
            elif state == "SUMINISTRANDO":
                self._set_colors(COLOR_VERDE)
                if data:
                    status_text = (
                        f"Driver {data.get('driver', 'N/A')}\n"
                        f"{data.get('kwh', '0.0')} kWh\n"
                        f"{data.get('eur', '0.00')} €"
                    )
                    self.lbl_status.configure(text=status_text)
            elif state == "PARADO":
                self._set_colors(COLOR_NARANJA)
                self.lbl_status.configure(text="Out of Order")
            elif state == "AVERIADO":
                self._set_colors(COLOR_ROJO)
                self.lbl_status.configure(text="")
            elif state == "DESCONECTADO":
                self._set_colors(COLOR_GRIS)
                self.lbl_status.configure(text="")
            else:
                print(f"(GUI) Estado desconocido para {self.cp_id}: {state}")
        except Exception as e:
            print(f"Error actualizando panel {self.cp_id}: {e}")

class CentralApp(tk.Tk):
   
    def __init__(self, gui_queue):
        super().__init__()
        self.title("SD EV CHARGING SOLUTION. MONITORIZATION PANEL")
        self.geometry("1000x700")
        self.configure(bg="#212121")

        self.gui_queue = gui_queue
        self.backend_controller = None
        self.cp_panels = {} 

        self._create_widgets()

        self.after(100, self._process_queue)

    def set_controller(self, controller):
        self.backend_controller = controller

    def _create_widgets(self):
        title_font = font.Font(family="Arial", size=16, weight="bold")
        lbl_title = tk.Label(
            self, 
            text="*** SD EV CHARGING SOLUTION. MONITORIZATION PANEL ***",
            bg="#0D47A1", 
            fg="white", 
            pady=10,
            font=title_font
        )
        lbl_title.pack(fill="x")

        self.grid_frame = tk.Frame(self, bg=COLOR_FONDO_REJILLA, padx=10, pady=10)
        self.grid_frame.pack(fill="x", expand=False)
        for i in range(5):
            self.grid_frame.columnconfigure(i, weight=1, uniform="cp_col")

        bottom_pane = tk.Frame(self, bg="#212121")

        admin_frame = self._create_admin_panel(self)
        
        admin_frame.pack(fill="x", side="bottom", pady=10, padx=10)
        
        bottom_pane.pack(fill="both", expand=True, pady=10)
        

        bottom_pane.columnconfigure(0, weight=1)
        bottom_pane.rowconfigure(0, weight=1)
        bottom_pane.rowconfigure(1, weight=1)

        requests_frame = self._create_requests_panel(bottom_pane)
        requests_frame.grid(row=0, column=0, sticky="nsew", padx=10, pady=(0, 5))

        messages_frame = self._create_messages_panel(bottom_pane)
        messages_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=(5, 0))

    def _create_requests_panel(self, parent):
        req_frame = tk.LabelFrame(
            parent, 
            text=" *** ON_GOING DRIVERS REQUESTS *** ", 
            fg="white", 
            bg="#333333",
            font=("Arial", 11, "bold")
        )
        
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("Treeview", 
                        background="#555555", 
                        foreground="white", 
                        fieldbackground="#555555",
                        rowheight=25)
        style.map('Treeview', background=[('selected', '#0D47A1')])
        style.configure("Treeview.Heading", 
                        background="#444444", 
                        foreground="white", 
                        font=("Arial", 10, "bold"))

        cols = ('DATE', 'START TIME', 'User ID', 'CP')
        self.requests_tree = ttk.Treeview(req_frame, columns=cols, show='headings')
        
        for col in cols:
            self.requests_tree.heading(col, text=col)
            self.requests_tree.column(col, width=150, anchor="center")
            
        self.requests_tree.pack(fill="both", expand=True, padx=5, pady=5)
        return req_frame

    def _create_messages_panel(self, parent):
        msg_frame = tk.LabelFrame(
            parent, 
            text=" *** APLICATION MESSAGES *** ", 
            fg="white", 
            bg="#333333",
            font=("Arial", 11, "bold")
        )
        
        self.messages_listbox = tk.Listbox(
            msg_frame, 
            bg="#555555", 
            fg="white", 
            selectbackground="#0D47A1",
            font=("Consolas", 10)
        )
        self.messages_listbox.pack(fill="both", expand=True, padx=5, pady=5)
        return msg_frame

    # ESTA ES LA FUNCION QUE DIBUJA LOS BOTONES
    def _create_admin_panel(self, parent):
        admin_frame = tk.Frame(parent, bg="#333333", pady=5)
        
        tk.Label(admin_frame, text="Control Manual:", fg="white", bg="#333333", font=("Arial", 10, "bold")).pack(side="left", padx=10)
        
        tk.Label(admin_frame, text="CP ID:", fg="white", bg="#333333").pack(side="left", padx=(10, 0))
        self.admin_cp_entry = tk.Entry(admin_frame, width=10)
        self.admin_cp_entry.pack(side="left", padx=5)
        
        btn_parar = tk.Button(admin_frame, text="Parar CP", bg=COLOR_NARANJA, fg="white", command=self._on_parar_cp)
        btn_parar.pack(side="left", padx=5)
        
        btn_reanudar = tk.Button(admin_frame, text="Reanudar CP", bg=COLOR_VERDE, fg="white", command=self._on_reanudar_cp)
        btn_reanudar.pack(side="left", padx=5)
        
        self.admin_cp_entry.bind("<Return>", self._on_reanudar_cp)

        return admin_frame
        
    def load_initial_cps(self, cp_list):
        for cp_data in cp_list:
            panel = CPPanel(
                self.grid_frame, 
                cp_data["id"], 
                cp_data["loc"], 
                cp_data["price"]
            )
            panel.grid(
                row=cp_data["grid_row"], 
                column=cp_data["grid_col"], 
                sticky="nsew", 
                padx=5, 
                pady=5
            )
            self.cp_panels[cp_data["id"]] = panel

    def _update_cp_state(self, cp_id, state, data=None):
        if cp_id in self.cp_panels:
            self.cp_panels[cp_id].update_state(state, data)
        else:
            self._add_app_message(f"AVISO: Intento de actualizar CP desconocido: {cp_id}")
            
    def _add_request_log(self, date, start_time, user_id, cp):
        self.requests_tree.insert("", "end", values=(date, start_time, user_id, cp))
        
    def _add_app_message(self, message):
        self.messages_listbox.insert("end", message)
        self.messages_listbox.see("end") 

    def _on_parar_cp(self):
        cp_id = self.admin_cp_entry.get().strip() 

        if not cp_id:
            self._add_app_message("ERROR: Introduce un CP ID para parar.")
            return
        
        if self.backend_controller:
            self.backend_controller.request_parar_cp(cp_id)
        else:
            self._add_app_message("ERROR: Backend controller no está conectado.")

    def _on_reanudar_cp(self, event=None): 
        cp_id = self.admin_cp_entry.get().strip() 

        if not cp_id:
            self._add_app_message("ERROR: Introduce un CP ID para reanudar.")
            return
            
        if self.backend_controller:
            self.backend_controller.request_reanudar_cp(cp_id)
        else:
            self._add_app_message("ERROR: Backend controller no está conectado.")
        
    def _process_queue(self):
        try:
            while True:
                message = self.gui_queue.get_nowait()
                msg_type = message[0]
                
                if msg_type == "UPDATE_CP":
                    _, cp_id, state, data = message
                    self._update_cp_state(cp_id, state, data)
                elif msg_type == "ADD_REQUEST":
                    _, date, start, user, cp = message
                    self._add_request_log(date, start, user, cp)
                elif msg_type == "ADD_MESSAGE":
                    _, msg_text = message
                    self._add_app_message(msg_text)

        except queue.Empty:
            pass
        finally:
            self.after(100, self._process_queue)