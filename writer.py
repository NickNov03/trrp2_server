import psycopg2
import psycopg2.extras
import analysis

class writer():
    def __init__(self, params):
        self.pg_conn = psycopg2.connect(
            dbname=  params["db_name"],
            user=    params["db_user"],
            password=params["db_password"],
            host=    params["db_host"],
            port=    params["db_port"]
        )
        
    def write(self, orders):
        with self.pg_conn.cursor() as self.pg_cursor:
            stage_ids = {}
            material_ids = {}

            for order in orders:
                id_order, address, work_stages, work_prices, materials, material_quantities, material_prices = order

                #### ЗАКАЗЫ ####
                self.pg_cursor.execute("INSERT INTO Orders (id_order, address) VALUES (%s, %s) ON CONFLICT (id_order) DO NOTHING", (id_order, address))

                #### ЭТАПЫ РАБОТ (и их цены) ####
                stages = work_stages.split(", ")
                stage_prices = work_prices.split(", ")
                
                for stage, price in zip(stages, stage_prices):
                    price = float(price)
                    
                    # Если еще не добавляли этот этап
                    if stage not in stage_ids:
                        self.pg_cursor.execute("INSERT INTO WorkStages (stage_name, price) VALUES (%s, %s) ON CONFLICT (stage_name) DO NOTHING RETURNING id_stage", (stage, price))

                        # ID добавленного этапа работ
                        stage_id = self.pg_cursor.fetchone()

                        # Если id_stage не был возвращён (если мы его уже добавили ранее и случился конфликт), запрашиваем его вручную
                        # иначе м.б. ситуация, что он уже был в Postgre, а мы не добавили его в словарь
                        if not stage_id:
                            self.pg_cursor.execute("SELECT id_stage FROM WorkStages WHERE stage_name = %s", (stage,))
                            stage_id = self.pg_cursor.fetchone()

                        # Добавляем в словарь, если получили ID
                        if stage_id:
                            stage_ids[stage] = stage_id[0]
                        else:
                            print(f"Ошибка: не найден ID для этапа '{stage}'")


                    # Связываем заказ с этапами работ
                    self.pg_cursor.execute("INSERT INTO Orders_WorkStages (id_order, id_stage) VALUES (%s, %s) ON CONFLICT DO NOTHING", (id_order, stage_ids[stage]))

                #### МАТЕРИАЛЫ (их количество и цены) ####
                materials_list = materials.split(", ")
                material_quantities = material_quantities.split(", ")
                material_prices = material_prices.split(", ")

                for material, quantity, price in zip(materials_list, material_quantities, material_prices):
                    quantity = int(quantity)
                    
                    # Если материал не в словаре
                    if material not in material_ids:
                        self.pg_cursor.execute("INSERT INTO Materials (material_name, price) VALUES (%s, %s) ON CONFLICT (material_name) DO NOTHING RETURNING id_material", \
                                        (material, price))

                        # ID материала
                        material_id = self.pg_cursor.fetchone()

                        # Если id_material не был возвращён (конфликт), запрашиваем его вручную
                        if not material_id:
                            # считается, что материал не изменяется в цене
                            self.pg_cursor.execute("SELECT id_material FROM Materials WHERE material_name = %s", (material,))
                            material_id = self.pg_cursor.fetchone()

                        # Добавляем в словарь
                        if material_id:
                            material_ids[material] = material_id[0]
                        else:
                            print(f"Ошибка: не найден ID для материала '{material}'")


                    # Связываем этапы работ с материалами
                    for stage in stages:
                        self.pg_cursor.execute("INSERT INTO WorkStages_Materials (id_stage, id_material, quantity) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", 
                                        (stage_ids[stage], material_ids[material], quantity))

            # Сохраняем изменения и закрываем соединения
            self.pg_conn.commit()
            self.pg_cursor.close()
            self.pg_conn.close()

            print("Импорт завершён успешно!")

            analysis.run_js_analysis(orders)