import subprocess

# Ejecutar el script de descarga y procesamiento

print("INICIADO PROCESO 1")
subprocess.run(["python3", "ps_1_dwld_input.py"], check=True)
print("INICIADO PROCESO 2")
subprocess.run(["python3", "ps_2_process_input.py"], check=True)
print("INICIADO PROCESO 3")
subprocess.run(["python3", "ps_3_run_model.py"], check=True)
print("INICIADO PROCESO 4")
subprocess.run(["python3", "ps_4_reglas_negocio.py"], check=True)
print("INICIADO PROCESO 5")
subprocess.run(["python3", "ps_5_subir_a_SF.py"], check=True)

# Crear Requirements
print("Creando requirements.txt")
subprocess.run(["pip", "freeze"], stdout=open("requirements.txt", "w"))
print("DONE requirements.txt")