import data_import

menu_options = {
    1: 'Import measurements',
    2: 'Exit',
}

def print_menu():
    for key in menu_options.keys():
        print(key, '--', menu_options[key])

def import_measurements():
  fk_measurement_station_id = input("Enter the value for fk_measurement_station_id: ")
  measurements_file = input("Enter the measurements txt file name: ")

  db = data_import.TimescaleDB()
  db.load_measurements(measurements_file, fk_measurement_station_id)
  db.close_connection()

while (True):
    print_menu()
    selected_option = ''
    try:
        selected_option = int(input('Enter your choice: '))
    except:
        print('Invalid input. Please enter a number...')
    if selected_option == 1:
        import_measurements()
    elif selected_option == 2:
        exit()
    else:
        print('Invalid option. Please enter an existing option')
