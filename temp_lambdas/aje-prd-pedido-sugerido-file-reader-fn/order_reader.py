import csv
from datetime import datetime

DELIMITER = ';'

class Fields:
    header_fields = ['Pais', 'Compania', 'Sucursal', 'Fecha', 'Modulo', 'Cliente', 'Destacar']
    # header_fields = ['Pais', 'Compania', 'Sucursal', 'Fecha', 'Modulo', 'Cliente']
    product_types = {
        'Producto':str, 
        'Cajas': int, 
        'Unidades': int,
        'tipoRecomendacion': str,
        'ultFecha': str # no olvidar de agregar "," arriba
    }
    date_format = {
        'field': header_fields[3],
        'format': '%Y-%m-%d',
        'message': 'no tiene el formato YYYY-MM-DD'
    }
    product_fields = list(product_types.keys())
    expected_fields = header_fields + product_fields
    
    @staticmethod
    def format_header_data(header_data):
        header_dict = {}
        for idx, value in enumerate(header_data):
            header_dict[Fields.header_fields[idx]] = value
        return header_dict


class OrdersFileReader:
    
    def __init__(self, logger):
        self.delimiter = ','
        self.line_errors = []
        self.summary = []
        self.lines = []
        self.logger = logger
    
    def validate_file(self, s3_obj):        
        file_read = s3_obj['Body'].read()

        # # Old: Como lo tenía tuxpas (LENTO)
        # encoding = chardet.detect(file_read)['encoding']
        # self.logger.info(f"encoding: {encoding}")
        # lines = file_read.decode(encoding).splitlines()

        # NEW CODE (FASTER & NO CHARDET)
        try:
            encoding = 'utf-8'
            decoded_content = file_read.decode(encoding)
            self.logger.info("Decodificado con UTF-8")
        except UnicodeDecodeError:
            # INTENTO SECUNDARIO (Excel/Windows default)
            try:
                encoding = 'iso-8859-1'
                decoded_content = file_read.decode(encoding)
                self.logger.info("Decodificado con ISO-8859-1")
            except UnicodeDecodeError:
                # Si falla ambos, lanzamos un error claro
                self.logger.error("Error: El archivo no es UTF-8 ni ISO-8859-1")
                raise Exception("Formato de archivo no soportado (use UTF-8 o Excel CSV)")

        lines = decoded_content.splitlines()
        # --------------------
        
        header = lines[0]

        if len(header.split(self.delimiter)) < 2:
            self.delimiter = ';'

        if self.has_headers_error(lines) or self.has_invalid_values(lines):
            summary_message = "Se presentaron los siguientes errores: " + ', '.join(set(self.summary))
            return {
                'error': True, 
                'messages': self.line_errors,
                'summary': summary_message
            }

        self.lines = lines
        return {'error': False}

    def has_headers_error(self, lines):
        header = lines[0].split(self.delimiter)

        self.logger.info(f"Delimitador detectado: '{self.delimiter}'")
        self.logger.info(f"Columnas detectadas en el CSV: {header}")
        self.logger.info(f"Columnas esperadas por el código: {Fields.expected_fields}")
        
        for field in Fields.expected_fields:
            if field not in header:
                error_message = f"Campo {field} no encontrado."
                self.line_errors.append(error_message)
                self.summary.append(error_message)
        return len(self.line_errors) > 0
        
    def has_value(self, line_dict):
        for field in Fields.expected_fields:
            if line_dict[field].strip() != '':
                return True
        return False
    
    def has_invalid_values(self, lines):

        date_field = Fields.date_format['field']
        date_format = Fields.date_format['format']
        date_message = Fields.date_format['message']

        for line_num, line in enumerate(csv.DictReader(
            lines, 
            delimiter=self.delimiter
        )):
            if not self.has_value(line):
                continue

            line_error = []
            
            date_value = line[date_field]
            try:
                datetime.strptime(date_value, date_format)
            except ValueError:
                line_error.append(f"{date_field} '{date_value}' {date_message}")
                self.summary.append(f"{date_field} {date_message}")

            for field in Fields.product_types:
                field_type = Fields.product_types[field]
                try:
                    field_type(line[field])
                except ValueError:
                    line_error.append(f"{field} '{line[field]}' no es del tipo entero")
                    self.summary.append(f"{field} no es del tipo entero")
                    
            if len(line_error) > 0:
                error_string = f"{line_num + 2}{DELIMITER}{DELIMITER.join(line_error)}"
                self.line_errors.append(error_string)

        return len(self.line_errors) > 0
    
    def get_orders(self):

        orders = {}
        for line in csv.DictReader(self.lines, delimiter=self.delimiter):
            # print(f"Línea leída por DictReader: {line}")
            if not self.has_value(line):
                continue

            header = tuple([line[field] for field in Fields.header_fields])

            if header not in orders:
                orders[header] = []

            orders[header].append({field:line[field] for field in Fields.product_fields})

        company_orders = {}
        for header, products in orders.items():
            company_info = tuple(header[:4]) # Posiciones de Pais, Compania, sucursal y fecha
            if company_info not in company_orders:
                company_orders[company_info] = []
            cleaned_order = {field:value for field, value in zip(Fields.header_fields, header)}

            # --- NUEVA LÓGICA ---
            # 1. Ajustar Destacar en el encabezado (cleaned_order)
            if "Destacar" in cleaned_order:
                val = str(cleaned_order["Destacar"]).lower().strip()
                cleaned_order["Destacar"] = (val == "true")
            else:
                cleaned_order["Destacar"] = False

            # 2. Ajustar ultFecha en cada producto de la lista
            for p in products:
                if "ultFecha" in p:
                    if not p["ultFecha"] or str(p["ultFecha"]).strip() == "":
                        del p["ultFecha"] # Evita el error de formato "" en Salesforce
            # ---------------
            
            cleaned_order["Productos"] = products            
            # Se comenta para que tome el valor de la nueva columna Destacar
            # cleaned_order["Destacar"] = False

            company_orders[company_info].append(cleaned_order)

        return company_orders