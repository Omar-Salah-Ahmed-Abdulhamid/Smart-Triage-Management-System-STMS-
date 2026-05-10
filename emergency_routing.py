import csv
import math
import time
import random
import threading
import multiprocessing
import concurrent.futures
from multiprocessing import Process, Queue, cpu_count
import tkinter as tk
import customtkinter as ctk
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# ==========================================
# 1. Helper Functions & Math (Core Logic)
# ==========================================
def calculate_distance(loc1, loc2):
    return math.sqrt((loc1[0] - loc2[0])**2 + (loc1[1] - loc2[1])**2) #فيثا 

def find_nearest_node(target_loc, nodes_list):
    distances = [(calculate_distance(target_loc, (n[0], n[1])), n) for n in nodes_list]
    min_dist, nearest_node = min(distances, key=lambda x: x[0])
    return min_dist, nearest_node

# MODULE LEVEL WORKER FOR REAL-TIME PARALLEL SIMULATION
def worker_calc_distances(sos_chunk, ambulances_data):
    results = []
    for sos in sos_chunk:
        dists = [(calculate_distance(sos, (a[0], a[1])), a, sos) for a in ambulances_data]
        if dists:
            best = min(dists, key=lambda x: x[0])
            results.append(best) 
    return results

def generate_emergency_data(filename, num_records, is_hosp=False):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        for i in range(num_records):
            prefix = "HOSP" if is_hosp else "PAT"
            writer.writerow([random.uniform(0, 1000), random.uniform(0, 1000), f"{prefix}_{i}"])

def load_hospitals(filename):
    hospitals = []
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            hospitals.append((float(row[0]), float(row[1]), row[2]))
    return hospitals

# ==========================================
# 2. Benchmark Systems (Batch Processing)
# ==========================================
def sequential_routing(patients_file, output_file, hospitals):
    with open(patients_file, 'r') as f_in, open(output_file, 'w', newline='') as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        writer.writerow(["Patient_X", "Patient_Y", "Nearest_Hospital_ID", "Distance"])
        for row in reader:
            patient_loc = (float(row[0]), float(row[1]))
            min_dist, nearest_hosp = find_nearest_node(patient_loc, hospitals)
            writer.writerow([patient_loc[0], patient_loc[1], nearest_hosp[2], min_dist])

def stage1_producer(patients_file, task_q, num_workers, total_patients):
    chunk_size = max(1, total_patients // (num_workers * 2)) 
    with open(patients_file, 'r') as f:
        reader = csv.reader(f)
        chunk = []
        for row in reader:
            chunk.append((float(row[0]), float(row[1])))
            if len(chunk) == chunk_size:
                task_q.put(chunk)
                chunk = []
        if chunk: task_q.put(chunk)
    for _ in range(num_workers): task_q.put(None)

def stage2_worker(task_q, result_q, hospitals):
    while True:
        chunk = task_q.get()
        if chunk is None:
            result_q.put(None)
            break
        result_chunk = []
        for patient_loc in chunk:
            min_dist, nearest_hosp = find_nearest_node(patient_loc, hospitals)
            result_chunk.append((patient_loc, nearest_hosp[2], min_dist))
        result_q.put(result_chunk)

def stage3_consumer(output_file, result_q, num_workers):
    finished_workers = 0
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Patient_X", "Patient_Y", "Nearest_Hospital_ID", "Distance"])
        while finished_workers < num_workers:
            result_chunk = result_q.get()
            if result_chunk is None:
                finished_workers += 1
            else:
                for res in result_chunk:
                    patient_loc, nearest_hosp_id, min_dist = res
                    writer.writerow([patient_loc[0], patient_loc[1], nearest_hosp_id, min_dist])

# ==========================================
# 3. Premium Hybrid GUI
# ==========================================
ctk.set_appearance_mode("Light")

BG_WORKSPACE = "#EAECEE"    
BG_SIDEBAR = "#1E2024"      
BG_CARD_WHITE = "#FFFFFF"   
BG_CARD_DARK = "#26282E"    
TEXT_DARK = "#1A1A1A"       
TEXT_LIGHT = "#FFFFFF"      
TEXT_MUTED = "#9BA1A6"      
ACCENT_LIME = "#D5F528"     
ACCENT_PURPLE = "#B19CFB"   
RED_CRESCENT = "#EF4444"    

class EmergencySystemApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("☾ Smart Triage: Emergency System")
        
        try:
            self.state('zoomed')
        except:
            self.attributes('-zoomed', True) 
            
        self.configure(fg_color=BG_WORKSPACE)
        self.grid_columnconfigure(0, weight=0, minsize=300) 
        self.grid_columnconfigure(1, weight=1) 
        self.grid_rowconfigure(0, weight=1)

        self.setup_sidebar()
        self.setup_workspace()
        
        self.hospitals_cache = [] 
        self.ambulances_cache = []
        self.pending_sos = [] 
        self.active_sos_dots = {} 
        self.is_dispatching = False 

    def setup_sidebar(self):
        self.sidebar = ctk.CTkFrame(self, fg_color=BG_SIDEBAR, corner_radius=20)
        self.sidebar.grid(row=0, column=0, sticky="nsew", padx=(10, 0), pady=10)

        header = ctk.CTkFrame(self.sidebar, fg_color="transparent")
        header.pack(pady=35, padx=20, fill="x")
        ctk.CTkLabel(header, text="☾", font=("Segoe UI", 52), text_color=RED_CRESCENT).pack(side="left", padx=(0, 10))
        
        title_frame = ctk.CTkFrame(header, fg_color="transparent")
        title_frame.pack(side="left", fill="y", pady=(8, 0)) 
        ctk.CTkLabel(title_frame, text="Smart Triage", font=("Segoe UI", 18, "bold"), text_color=TEXT_LIGHT).pack(anchor="w")
        ctk.CTkLabel(title_frame, text="Emergency System", font=("Segoe UI", 13), text_color=TEXT_MUTED).pack(anchor="w")

        menu_frame = ctk.CTkFrame(self.sidebar, fg_color="transparent")
        menu_frame.pack(fill="x", padx=20, pady=10)
        active_item = ctk.CTkFrame(menu_frame, fg_color=BG_CARD_WHITE, corner_radius=25, height=45)
        active_item.pack(fill="x", pady=5)
        active_item.pack_propagate(False)
        ctk.CTkLabel(active_item, text="❖", font=("Segoe UI", 16), text_color=TEXT_DARK).pack(side="left", padx=(20, 10))
        ctk.CTkLabel(active_item, text="Overview", font=("Segoe UI", 14, "bold"), text_color=TEXT_DARK).pack(side="left")
        
        inputs_frame = ctk.CTkFrame(self.sidebar, fg_color="transparent")
        inputs_frame.pack(pady=30, padx=25, fill="x")
        ctk.CTkLabel(inputs_frame, text="BATCH DATASET TEST", font=("Segoe UI", 11, "bold"), text_color=ACCENT_PURPLE).pack(anchor="w", pady=(0, 15))
        
        ctk.CTkLabel(inputs_frame, text="HOSPITAL NODES", font=("Segoe UI", 10, "bold"), text_color=TEXT_MUTED).pack(anchor="w", pady=(0, 5))
        self.entry_hosp = ctk.CTkEntry(inputs_frame, font=("Segoe UI", 14), fg_color="#2A2D34", border_width=0, height=40, text_color=TEXT_LIGHT, corner_radius=10)
        self.entry_hosp.insert(0, "15000")
        self.entry_hosp.pack(fill="x", pady=(0, 15))

        ctk.CTkLabel(inputs_frame, text="SOS SIGNALS (PATIENTS)", font=("Segoe UI", 10, "bold"), text_color=TEXT_MUTED).pack(anchor="w", pady=(0, 5))
        self.entry_pat = ctk.CTkEntry(inputs_frame, font=("Segoe UI", 14), fg_color="#2A2D34", border_width=0, height=40, text_color=TEXT_LIGHT, corner_radius=10)
        self.entry_pat.insert(0, "5000")
        self.entry_pat.pack(fill="x", pady=(0, 10))

        action_card = ctk.CTkFrame(self.sidebar, fg_color=ACCENT_LIME, corner_radius=20)
        action_card.pack(fill="x", padx=20, pady=20, side="bottom")
        ctk.CTkLabel(action_card, text="Ready to Launch", font=("Segoe UI", 16, "bold"), text_color=TEXT_DARK).pack(pady=(20, 5))
        self.lbl_status = ctk.CTkLabel(action_card, text="Run offline dataset benchmark", font=("Segoe UI", 11), text_color=TEXT_DARK)
        self.lbl_status.pack(pady=(0, 15))
        
        self.btn_run = ctk.CTkButton(action_card, text="Start Data Processing", font=("Segoe UI", 13, "bold"), 
                                     fg_color=BG_SIDEBAR, text_color=TEXT_LIGHT, hover_color="#111111", height=45, corner_radius=15,
                                     command=self.start_benchmark)
        self.btn_run.pack(fill="x", padx=15, pady=(0, 20))

    def setup_workspace(self):
        self.workspace = ctk.CTkFrame(self, fg_color="transparent")
        self.workspace.grid(row=0, column=1, sticky="nsew", padx=20, pady=10)
        self.workspace.grid_rowconfigure(0, weight=0) 
        self.workspace.grid_rowconfigure(1, weight=0) 
        self.workspace.grid_rowconfigure(2, weight=1) 
        self.workspace.grid_columnconfigure(0, weight=1)
        self.workspace.grid_columnconfigure(1, weight=2) 

        header = ctk.CTkFrame(self.workspace, fg_color="transparent")
        header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(10, 20), padx=10)
        ctk.CTkLabel(header, text="Triage Headquarters (HQ)", font=("Segoe UI", 24, "bold"), text_color=TEXT_DARK).pack(side="left")

        kpi_frame = ctk.CTkFrame(self.workspace, fg_color="transparent")
        kpi_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 20))
        kpi_frame.grid_columnconfigure((0,1,2), weight=1)

        self.card_seq = self.create_kpi_card(kpi_frame, "Sequential (Baseline)", "0.000s", ACCENT_PURPLE, 0)
        self.card_par = self.create_kpi_card(kpi_frame, "Parallel (Smart Engine)", "0.000s", ACCENT_LIME, 1)
        self.card_spd = self.create_kpi_card(kpi_frame, "System Speedup", "--", TEXT_DARK, 2)

        chart_container = ctk.CTkFrame(self.workspace, fg_color=BG_CARD_DARK, corner_radius=20)
        chart_container.grid(row=2, column=0, sticky="nsew", padx=(10, 10), pady=(0, 10))
        header_chart = ctk.CTkFrame(chart_container, fg_color="transparent")
        header_chart.pack(fill="x", padx=25, pady=(20, 0))
        ctk.CTkLabel(header_chart, text="Processing Latency", font=("Segoe UI", 16, "bold"), text_color=TEXT_LIGHT).pack(side="left")

        self.figure = Figure(figsize=(4, 3), facecolor=BG_CARD_DARK)
        self.ax = self.figure.add_subplot(111)
        self.ax.set_facecolor(BG_CARD_DARK)
        self.ax.tick_params(colors=TEXT_MUTED, labelsize=9)
        for spine in self.ax.spines.values(): spine.set_visible(False)
        self.canvas_chart = FigureCanvasTkAgg(self.figure, master=chart_container)
        self.canvas_chart.get_tk_widget().pack(fill="both", expand=True, padx=15, pady=15)

        radar_container = ctk.CTkFrame(self.workspace, fg_color=BG_CARD_WHITE, corner_radius=20)
        radar_container.grid(row=2, column=1, sticky="nsew", padx=(10, 10), pady=(0, 10))
        
        radar_header = ctk.CTkFrame(radar_container, fg_color="transparent")
        radar_header.pack(fill="x", padx=25, pady=(20, 5))
        
        ctk.CTkLabel(radar_header, text="Live Radar & Dynamic Testing", font=("Segoe UI", 16, "bold"), text_color=TEXT_DARK).pack(side="left")
        
        stats_frame = ctk.CTkFrame(radar_header, fg_color="transparent")
        stats_frame.pack(side="left", padx=20)
        
        self.lbl_sim_status = ctk.CTkLabel(stats_frame, text="Engine: IDLE", font=("Segoe UI", 11, "bold"), text_color=TEXT_MUTED, width=250, anchor="w")
        self.lbl_sim_status.pack(anchor="w")
        self.lbl_sim_latency = ctk.CTkLabel(stats_frame, text="Decision Time: 0.00 ms", font=("Segoe UI", 11), text_color=TEXT_MUTED, width=250, anchor="w")
        self.lbl_sim_latency.pack(anchor="w")

        controls_frame = ctk.CTkFrame(radar_header, fg_color="transparent")
        controls_frame.pack(side="right")
        
        ctk.CTkLabel(controls_frame, text="SOS Vol:", font=("Segoe UI", 12, "bold"), text_color=TEXT_DARK).pack(side="left", padx=5)
        self.entry_sim_sos = ctk.CTkEntry(controls_frame, width=60, height=32, corner_radius=10, border_color="#E2E8F0")
        self.entry_sim_sos.insert(0, "20") 
        self.entry_sim_sos.pack(side="left", padx=5)
        
        self.btn_sim = ctk.CTkButton(controls_frame, text="Trigger System", font=("Segoe UI", 12, "bold"), 
                                     fg_color=RED_CRESCENT, text_color=TEXT_LIGHT, hover_color="#B91C1C", height=32, corner_radius=15,
                                     command=self.trigger_mass_simulation)
        self.btn_sim.pack(side="left")

        self.canvas = tk.Canvas(radar_container, bg=BG_CARD_WHITE, highlightthickness=0)
        self.canvas.pack(fill="both", expand=True, padx=25, pady=(0, 25))

    def create_kpi_card(self, parent, title, value, dot_color, col):
        card = ctk.CTkFrame(parent, fg_color=BG_CARD_WHITE, corner_radius=20)
        card.grid(row=0, column=col, sticky="ew", padx=10)
        top_frame = ctk.CTkFrame(card, fg_color="transparent")
        top_frame.pack(fill="x", padx=20, pady=(20, 0))
        dot = ctk.CTkFrame(top_frame, fg_color=dot_color, width=12, height=12, corner_radius=6)
        dot.pack(side="left", padx=(0, 8))
        ctk.CTkLabel(top_frame, text=title, font=("Segoe UI", 12, "bold"), text_color=TEXT_MUTED).pack(side="left")
        val_lbl = ctk.CTkLabel(card, text=value, font=("Segoe UI", 34, "bold"), text_color=TEXT_DARK)
        val_lbl.pack(anchor="w", padx=20, pady=(5, 20))
        return val_lbl

    def start_benchmark(self):
        self.btn_run.configure(state="disabled")
        n_hosp = int(self.entry_hosp.get())
        n_pat = int(self.entry_pat.get())
        threading.Thread(target=self.run_logic, args=(n_hosp, n_pat), daemon=True).start()

    def update_ui(self, log=None, t_seq=None, t_par=None, speedup=None):
        if log: self.lbl_status.configure(text=log)
        if t_seq: self.card_seq.configure(text=f"{t_seq:.2f}s")
        if t_par: self.card_par.configure(text=f"{t_par:.2f}s")
        if speedup: self.card_spd.configure(text=f"{speedup:.2f}x")

    def update_chart(self, seq_time, par_time):
        self.ax.clear()
        self.ax.bar(['Sequential', 'Parallel (Smart)'], [seq_time, par_time], color=[ACCENT_PURPLE, ACCENT_LIME], width=0.4)
        self.ax.set_ylabel('Latency (s)', color=TEXT_MUTED, fontfamily="Segoe UI", fontsize=10)
        self.figure.canvas.draw()

    def run_logic(self, num_hosp, num_pat):
        try:
            self.after(0, self.update_ui, "Generating dataset...")
            generate_emergency_data('hospitals.csv', num_hosp, True)
            generate_emergency_data('patients.csv', num_pat, False)
            hospitals_data = load_hospitals('hospitals.csv')

            self.after(0, self.update_ui, "Running Sequential Pipeline...")
            t0 = time.time()
            sequential_routing('patients.csv', 'results_seq.csv', hospitals_data)
            t_seq = time.time() - t0

            self.after(0, self.update_ui, "Running Parallel Pipeline...")
            task_q = Queue(maxsize=100) 
            result_q = Queue(maxsize=100)
            num_workers = cpu_count()

            t1 = time.time()
            p_producer = Process(target=stage1_producer, args=('patients.csv', task_q, num_workers, num_pat))
            workers = [Process(target=stage2_worker, args=(task_q, result_q, hospitals_data)) for _ in range(num_workers)]
            p_consumer = Process(target=stage3_consumer, args=('results_par.csv', result_q, num_workers))

            p_producer.start()
            for w in workers: w.start()
            p_consumer.start()

            p_producer.join()
            for w in workers: w.join()
            p_consumer.join()
            t_par = time.time() - t1

            speedup = t_seq / t_par if t_par > 0 else 0
            self.after(0, self.update_ui, "Benchmark Completed", t_seq, t_par, speedup)
            self.after(0, self.update_chart, t_seq, t_par)
            self.after(0, self.init_radar_map)

        except Exception as e:
            self.after(0, self.update_ui, f"Error occurred: {e}")
        finally:
            self.after(0, lambda: self.btn_run.configure(state="normal"))

    # ==========================================
    # GIS RADAR & REAL-TIME SYSTEM APPLICATION
    # ==========================================
    def draw_hospital(self, x, y):
        self.canvas.create_rectangle(x-18, y-15, x+18, y+15, fill="#E2E8F0", outline="#94A3B8", width=2) 
        self.canvas.create_rectangle(x-6, y+5, x+6, y+15, fill="#475569", outline="") 
        self.canvas.create_rectangle(x-12, y-8, x-6, y-2, fill="#93C5FD", outline="") 
        self.canvas.create_rectangle(x+6, y-8, x+12, y-2, fill="#93C5FD", outline="") 
        self.canvas.create_text(x, y-22, text="☾", fill=RED_CRESCENT, font=("Segoe UI", 16, "bold")) 

    def draw_ambulance(self, x, y, amb_id):
        tag = f"amb_{amb_id}"
        self.canvas.create_rectangle(x-12, y-10, x+8, y+4, fill="#FFFFFF", outline="#CBD5E1", width=2, tags=tag) 
        self.canvas.create_rectangle(x+8, y-4, x+16, y+4, fill="#FFFFFF", outline="#CBD5E1", width=2, tags=tag) 
        self.canvas.create_oval(x-8, y+2, x-2, y+8, fill="#1E293B", outline="", tags=tag) 
        self.canvas.create_oval(x+6, y+2, x+12, y+8, fill="#1E293B", outline="", tags=tag) 
        self.canvas.create_text(x-2, y-3, text="☾", fill=RED_CRESCENT, font=("Segoe UI", 12, "bold"), tags=tag) 
        return tag

    def init_radar_map(self):
        self.canvas.delete("all")
        self.hospitals_cache = []
        self.ambulances_cache = []
        self.update_idletasks() 
        w, h = self.canvas.winfo_width(), self.canvas.winfo_height()
        
        for i in range(0, w, 40):
            for j in range(0, h, 40):
                self.canvas.create_oval(i, j, i+2, j+2, fill="#EAECEE", outline="")

        for i in range(12):
            hx, hy = random.randint(50, w-50), random.randint(50, h-50)
            self.draw_hospital(hx, hy)
            self.hospitals_cache.append((hx, hy, f"HOSP_EMERGENCY_{i}"))

        for i in range(8):
            ax, ay = random.randint(50, w-50), random.randint(50, h-50)
            tag = self.draw_ambulance(ax, ay, i)
            self.ambulances_cache.append({'tag': tag, 'x': ax, 'y': ay, 'busy': False, 'id': i})

    def trigger_mass_simulation(self):
        if not self.hospitals_cache: self.init_radar_map()
        
        self.canvas.delete("sim_path", "pending_patient", "info_ui")
        self.active_sos_dots = {}

        try:
            sos_count = int(self.entry_sim_sos.get())
        except:
            sos_count = 1

        w, h = self.canvas.winfo_width(), self.canvas.winfo_height()
        
        self.pending_sos = [(random.randint(60, w-60), random.randint(60, h-60)) for _ in range(sos_count)]
        
        draw_limit = min(sos_count, 500)
        for px, py in self.pending_sos[:draw_limit]:
            dot_id = self.canvas.create_oval(px-4, py-4, px+4, py+4, fill=TEXT_MUTED, outline="", tags="pending_patient")
            self.active_sos_dots[(px, py)] = dot_id

        self.dispatch_ambulances()

    def dispatch_ambulances(self):
        if not self.pending_sos:
            self.lbl_sim_status.configure(text="Engine: ALL CLEARED!", text_color=ACCENT_LIME)
            return

        if getattr(self, 'is_dispatching', False):
            return 
        self.is_dispatching = True

        threading.Thread(target=self._calculate_and_dispatch, daemon=True).start()

    def _calculate_and_dispatch(self):
        available_ambs = [a for a in self.ambulances_cache if not a['busy']]
        if not available_ambs:
            self.is_dispatching = False
            return 

        amb_data = [(a['x'], a['y'], a['id']) for a in available_ambs]
        sos_count = len(self.pending_sos)
        
        t_start = time.time()
        assignments = [] 
        
        if sos_count < 15:
            self.after(0, lambda: self.lbl_sim_status.configure(text=f"Engine: SEQUENTIAL ({sos_count} Left)", text_color=ACCENT_PURPLE))
            unassigned_sos = list(self.pending_sos)
            for amb in amb_data:
                if not unassigned_sos: break
                best_dist = float('inf')
                best_sos = None
                for sos in unassigned_sos:
                    d = calculate_distance((amb[0], amb[1]), sos)
                    if d < best_dist:
                        best_dist = d
                        best_sos = sos
                if best_sos:
                    assignments.append((amb[2], best_sos))
                    unassigned_sos.remove(best_sos)
        else:
            self.after(0, lambda: self.lbl_sim_status.configure(text=f"Engine: PARALLEL THREADS ({sos_count} Left)", text_color=ACCENT_LIME))
            chunks = [self.pending_sos[i::cpu_count()] for i in range(cpu_count())]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()) as executor:
                futures = [executor.submit(worker_calc_distances, chunk, amb_data) for chunk in chunks if chunk]
                results = [f.result() for f in futures]
                
            all_matches = [match for res_list in results for match in res_list]
            all_matches.sort(key=lambda x: x[0]) 
            
            assigned_amb_ids = set()
            assigned_sos = set()
            
            for dist, amb, sos in all_matches:
                if amb[2] not in assigned_amb_ids and sos not in assigned_sos:
                    assignments.append((amb[2], sos))
                    assigned_amb_ids.add(amb[2])
                    assigned_sos.add(sos)
                    if len(assigned_amb_ids) == len(amb_data): break

        t_end = time.time()
        latency_ms = (t_end - t_start) * 1000
        self.after(0, lambda: self.lbl_sim_latency.configure(text=f"Decision Time: {latency_ms:.2f} ms"))

        self.after(0, self._apply_dispatch, assignments)

    def _apply_dispatch(self, assignments):
        for amb_id, target_sos in assignments:
            if target_sos in self.pending_sos:
                self.pending_sos.remove(target_sos)

            px, py = target_sos
            
            original_dot_id = self.active_sos_dots.get((px, py))
            if original_dot_id:
                self.canvas.itemconfig(original_dot_id, fill=RED_CRESCENT)

            ping = self.canvas.create_oval(px-15, py-15, px+15, py+15, outline=RED_CRESCENT, width=2, tags="sim_path")
            
            best_amb = next(a for a in self.ambulances_cache if a['id'] == amb_id)
            best_amb['busy'] = True
            start_x, start_y = best_amb['x'], best_amb['y']
            
            line = self.canvas.create_line(start_x, start_y, px, py, fill=TEXT_MUTED, dash=(4, 4), width=2, tags="sim_path")
            
            info_bg = self.canvas.create_rectangle(start_x-55, start_y-40, start_x+55, start_y-20, fill=TEXT_DARK, outline="", tags="info_ui")
            info_text = self.canvas.create_text(start_x, start_y-30, text="En Route", fill=BG_CARD_WHITE, font=("Segoe UI", 8, "bold"), tags="info_ui")

            self.animate_movement(best_amb, start_x, start_y, px, py, 0, line, ping, info_text, info_bg, original_dot_id, phase=1)

        self.is_dispatching = False

    def animate_movement(self, amb_obj, start_x, start_y, target_x, target_y, step, line, ping, info_text, info_bg, pat_dot, phase):
        total_steps = 40 
        if step <= total_steps:
            dx = (target_x - start_x) / total_steps
            dy = (target_y - start_y) / total_steps
            
            self.canvas.move(amb_obj['tag'], dx, dy)
            
            curr_x = start_x + (dx * step)
            curr_y = start_y + (dy * step)
            self.canvas.coords(info_bg, curr_x-55, curr_y-40, curr_x+55, curr_y-20)
            self.canvas.coords(info_text, curr_x, curr_y-30)
            
            if phase == 1: 
                pulse = (step % 10) - 5 # قيمة متغيرة لخلق التأثير
                self.canvas.coords(ping, target_x-15-pulse, target_y-15-pulse, target_x+15+pulse, target_y+15+pulse)
                self.canvas.itemconfig(ping, outline=BG_CARD_WHITE if step % 10 < 5 else RED_CRESCENT)
                
            self.after(30, self.animate_movement, amb_obj, start_x, start_y, target_x, target_y, step+1, line, ping, info_text, info_bg, pat_dot, phase)
        else:
            if phase == 1: 
                self.canvas.delete(ping, line)
                if pat_dot: self.canvas.delete(pat_dot) 

                self.canvas.itemconfig(info_text, text="Secured", fill=ACCENT_LIME)
                
                _, best_hosp = find_nearest_node((target_x, target_y), self.hospitals_cache)
                hx, hy, hosp_id = best_hosp
                
                new_line = self.canvas.create_line(target_x, target_y, hx, hy, fill="#3B82F6", dash=(4, 4), width=2, tags="sim_path")
                hosp_highlight = self.canvas.create_oval(hx-25, hy-25, hx+25, hy+25, outline="#3B82F6", width=2, tags="sim_path")
                
                self.after(600, self.animate_movement, amb_obj, target_x, target_y, hx, hy, 0, new_line, hosp_highlight, info_text, info_bg, None, 2)
                
            elif phase == 2: 
                self.canvas.delete(line, ping) 
                self.canvas.itemconfig(info_text, text="Arrived", fill=ACCENT_LIME)
                
                amb_obj['x'], amb_obj['y'], amb_obj['busy'] = target_x, target_y, False
                self.after(2000, lambda: self.canvas.delete(info_text, info_bg))

                self.dispatch_ambulances()

if __name__ == "__main__":
    multiprocessing.freeze_support()
    app = EmergencySystemApp()
    app.mainloop()
