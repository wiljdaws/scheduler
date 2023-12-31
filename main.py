from datetime import datetime, timedelta
import getpass
from operator import call
import os
import shutil
import sys
from xmlrpc.client import boolean
from numpy import spacing
import schedule
import time
import json
import logging
from tabulate import tabulate
from dateutil.parser import parse
import colorama
from colorama import Fore, Style
import time

num = 20
terminal_width = shutil.get_terminal_size().columns
spacing = int((terminal_width-15)/4)-3 
frames = [
    " "* spacing + "╔════╤"+"╤╤"*num+"╤════╗\n" +
    " "* spacing + "║    │"+"││"*num+" \\   ║\n" +
    " "* spacing + "║    │"+"││"*num+"  O  ║\n" +
    " "* spacing + "║    O"+"OO"*num+"     ║",

    " "* spacing + "╔════╤"+"╤╤"*num+"╤════╗\n" +
    " "* spacing + "║    │"+"││"*num+"│    ║\n" +
    " "* spacing + "║    │"+"││"*num+"│    ║\n" +
    " "* spacing + "║    O"+"OO"*num+"O    ║",

    " "* spacing + "╔════╤╤"+"╤╤"*num+"════╗\n" +
    " "* spacing + "║   / │"+"││"*num+"    ║\n" +
    " "* spacing + "║  O  │"+"││"*num+"    ║\n" +
    " "* spacing + "║     O"+"OO"*num+"    ║",

    " "* spacing + "╔════╤"+"╤╤"*num+"╤════╗\n" +
    " "* spacing + "║    │"+"││"*num+"│    ║\n" +
    " "* spacing + "║    │"+"││"*num+"│    ║\n" +
    " "* spacing + "║    O"+"OO"*num+"O    ║"
]

logging.basicConfig(filename='scheduler.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logging.info("Scheduler started")

user = getpass.getuser()
Desktop = os.path.expanduser("~") + r'\Desktop'
bat_folder = Desktop+r'\Bat Files'

class TaskScheduler:
    """
    A task scheduler for a data analyst to run programs for different departments based on their shift times.

    Attributes:
        tasks (dict): A dictionary to store tasks for each department and shift.
        shift_times (dict): A dictionary containing the shift times for different departments and shifts.
        next_run_times (dict): A dictionary to store the next run time for each task.

    Methods:
        add_task: Add a task to the scheduler.
        remove_task: Remove a task from the scheduler.
        view_scheduled_tasks: View the scheduled tasks in a table format.
        run: Run the scheduler.
        stop: Stop the scheduler.
        clear_console: Clear the console based on the OS.
        display_animation: Display the animation in the console.
    """

    def __init__(self, shift_times: dict = None):
        self.tasks = {}
        self.shift_times = shift_times if shift_times is not None else {}
        self.next_run_times = {}  

    def is_valid_time_range(self, start_time: str, end_time: str)-> boolean:
        """
        Check if the time range is valid, accounting for 'Days' and 'Nights' shifts.

        Args:
            start_time (str): The start time in 'HH:MM' format.
            end_time (str): The end time in 'HH:MM' format.

        Returns:
            bool: True if the time range is valid, False otherwise.
        """
        start_time_obj = datetime.strptime(start_time, '%H:%M')
        end_time_obj = datetime.strptime(end_time, '%H:%M')

        if self.shift == 'Days':
            return end_time_obj >= start_time_obj
        elif self.shift == 'Nights':
            if start_time_obj < end_time_obj:
                return True
            next_day_end_time_obj = end_time_obj + timedelta(days=1)
            return next_day_end_time_obj >= start_time_obj
        return False
    
    def is_night_shift(self, shift: str)-> boolean:
        """
        Check if the given shift is a night shift.

        Args:
            shift (str): The shift to check.

        Returns:
            bool: True if it's a night shift, False otherwise.
        """
        return shift == 'Nights'
        
    def _get_next_occurrence_of_shift(self, shift_info: dict)-> tuple():
        """
        Get the next occurrence of a shift based on the current time.

        Args:
            shift_info (dict): A dictionary containing the 'start_time' and 'end_time' for the shift.

        Returns:
            datetime: The next occurrence of the shift as a datetime object.
            datetime: The start time of the shift as a datetime object.
            datetime: The end time of the shift as a datetime object.
        """
        now = datetime.now()
        start_time = datetime.strptime(shift_info['start_time'], '%H:%M')
        end_time = datetime.strptime(shift_info['end_time'], '%H:%M')

        if now < start_time:
            return now, start_time, end_time
        elif start_time <= now <= end_time:
            return now, start_time, end_time
        else:
            next_day = now + timedelta(days=1)
            return next_day.replace(hour=start_time.hour, minute=start_time.minute), start_time, end_time
    
    def get_task_by_department_shift_frequency(self, department: str, shift: str, frequency: str)-> dict:
        """
        Get the task from the scheduler based on department, shift, and frequency.

        Args:
            department (str): The department of the task.
            shift (str): The shift of the task.
            frequency (str): The frequency of the task.

        Returns:
            dict: The task dictionary if found, None otherwise.
        """
        key = (department, shift)
        if key in self.tasks:
            for task in self.tasks[key]:
                if task['frequency'] == frequency:
                    return task
        return None

    def add_task(self, department: str, shift: str, frequency: str, program: callable, start_time: str, end_time: str):
        """
        Add a task to the scheduler.

        Args:
            department (str): The department for which the task is added.
            shift (str): The shift for which the task is added.
            frequency (str): The frequency of the task. Choose from 'hourly', 'daily', or 'quarterly'.
            program (callable): The program/function to be executed.
            start_time (str): The start time of the shift in 'HH:MM' format.
            end_time (str): The end time of the shift in 'HH:MM' format.

        Raises:
            ValueError: If an invalid frequency is provided, the task overlaps with an existing one,
                        or daily_time is provided for non-daily tasks.
        """
        if frequency not in ['hourly', 'daily', 'quarterly']:
            raise ValueError("Invalid frequency. Please choose from 'hourly', 'daily', or 'quarterly'.")

        self.shift = shift 

        if frequency == 'daily':
            raise ValueError("Daily tasks are not supported in this version of the scheduler.")
        elif not self.is_valid_time_range(start_time, end_time):
            raise ValueError(f"End time should be after start time.\n\t department: {department}\t shift: {shift} \n\t start_time: {start_time}\t end_time: {end_time}")

        key = (department, shift)
        if key not in self.tasks:
            self.tasks[key] = []

        for existing_task in self.tasks[key]:
            if existing_task['frequency'] == frequency and not (end_time <= existing_task['start_time'] or start_time >= existing_task['end_time']):
                raise ValueError("Task overlaps with an existing one.")

        self.tasks[key].append({'frequency': frequency, 'program': program, 'start_time': start_time, 'end_time': end_time})

    def add_task_for_department_shift(self, department: str, shift: str, frequency:str, program: callable):
        """
        Add a task to the scheduler for a specific department and shift.

        Args:
            department (str): The department for which the task is added.
            shift (str): The shift for which the task is added.
            frequency (str): The frequency of the task. Choose from 'hourly', 'daily', or 'quarterly'.
            program (callable): The program/function to be executed.

        Raises:
            ValueError: If an invalid frequency is provided or the task overlaps with an existing one.
        """
        frequency = frequency.lower()
        if frequency not in ['hourly', 'daily', 'quarterly']:
            raise ValueError("Invalid frequency. Please choose from 'hourly', 'daily', or 'quarterly'.")

        key = (department, shift)
        if key not in self.tasks:
            self.tasks[key] = []

        shift_times = self.shift_times.get(department, {}).get(shift, {})
        start_time = shift_times.get('start_time')
        end_time = shift_times.get('end_time')

        shift = shift.capitalize()
        if shift == 'Days' and start_time >= end_time:
            raise ValueError(f"End time should be after start time.\n\t department: {department}\t shift: {shift} \n\t start_time: {start_time}\t end_time: {end_time}")
        elif shift == 'Nights' and start_time <= end_time:
            raise ValueError(f"Start time should be after end time.\n\t start_time: {start_time} end_time: {end_time}")
        else:
            for existing_task in self.tasks[key]:
                if existing_task['frequency'] == frequency and not (end_time <= existing_task['start_time'] or start_time >= existing_task['end_time']):
                    raise ValueError("Task overlaps with an existing one.")
            task = {'department': department, 'shift': shift, 'frequency': frequency, 'program': program, 'start_time': start_time, 'end_time': end_time}
            self.tasks[key].append(task)

    def remove_task(self, department:str, shift:str, frequency:str):
        """
        Remove a task from the scheduler.

        Args:
            department (str): The department for which the task is removed.
            shift (str): The shift for which the task is removed.
            frequency (str): The frequency of the task. Choose from 'hourly', 'daily', or 'quarterly'.
        """
        key = (department, shift)
        if key in self.tasks:
            self.tasks[key] = [task for task in self.tasks[key] if task['frequency'] != frequency]

    def view_scheduled_tasks(self)-> str:
        '''
            Displays the scheduled tasks in a table format.
            
            Returns:
                str: The scheduled tasks in a table format.
        '''
        tasks_table = []
        headers = ["Department", "Shift", "Frequency", "Program", "Start Time", "End Time", "Next Run Time"]
        for (department, shift), tasks in self.tasks.items():
            for task in tasks:
                frequency = task['frequency']
                try:
                    program = task['program'].__name__
                except AttributeError as e:
                    program = task['program']
                start_time = task['start_time']
                end_time = task['end_time']
                next_run_time = self._get_next_run_time(task, department, shift)
                self._schedule_task(program, task)
                next_run_time = next_run_time.strftime('%m/%d/%Y %H:%M')
                tasks_table.append([department, shift, frequency, program, start_time, end_time, next_run_time])

        return tabulate(tasks_table, headers=headers, tablefmt="grid")

    def _schedule_task(self, program: callable, task: dict):
        '''
            Schedules the task for the shift at the designated department
            
            Args:
                program (callable): The program/function to be executed.
                task (dict): The task dictionary containing the department, shift, and frequency.
        '''
        department = task['department']
        shift = task['shift']
        frequency = task['frequency']
        start_time = task['start_time']
        end_time = task['end_time']

        next_run_time = None
        next_occurrence, _, _ = self._get_next_occurrence_of_shift(self.shift_times[department][shift])

        if frequency == 'hourly':
            if self.is_night_shift(shift):
                start_time_dt = datetime.strptime(start_time, '%H:%M')
                end_time_dt = datetime.strptime(end_time, '%H:%M')
                while start_time_dt <= end_time_dt:
                    adjusted_start_time = start_time_dt.strftime('%H:%M')
                    task['department'] = department
                    self.next_run_times[(department, shift, frequency)] = start_time_dt  
                    schedule.every().hour.at(adjusted_start_time).do(self._execute_task, program, task)
                    next_hour = start_time_dt.replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)
                    start_time_dt = start_time_dt.replace(hour=next_hour.hour)
                next_occurrence, _, _ = self._get_next_occurrence_of_shift(self.shift_times[department]['Days'])
                while next_occurrence <= next_occurrence.replace(hour=7, minute=0):
                    adjusted_start_time = next_occurrence.strftime('%H:%M')
                    task['department'] = department
                    self.next_run_times[(department, shift, frequency)] = next_occurrence  
                    schedule.every().hour.at(adjusted_start_time).do(self._execute_task, program, task)
                    next_hour = next_occurrence.replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)
                    next_occurrence = next_occurrence.replace(hour=next_hour.hour)
            else:
                interval = timedelta(hours=1)
                current_time = datetime.now().replace(second=0, microsecond=0)
                next_run_time = current_time + interval

                while next_run_time <= datetime.now().replace(hour=int(end_time[:2]), minute=int(end_time[3:]), second=0, microsecond=0):
                    if next_run_time >= datetime.now().replace(hour=int(start_time[:2]), minute=int(start_time[3:]), second=0, microsecond=0):
                        adjusted_start_time = next_run_time.strftime('%H:%M')
                        task['department'] = department
                        self.next_run_times[(department, shift, frequency)] = next_run_time  
                        schedule.every().hour.at(adjusted_start_time).do(self._execute_task, program, task)

                    next_run_time += interval
        elif frequency == 'daily':
            raise ValueError("Daily tasks are not supported in this version of the scheduler.")
        
        elif frequency == 'quarterly':
            if self.is_night_shift(shift):
                start_time_dt = datetime.strptime(start_time, '%H:%M')
                end_time_dt = datetime.strptime(end_time, '%H:%M')
                while start_time_dt <= end_time_dt:
                    adjusted_start_time = start_time_dt.strftime('%H:%M')
                    task['department'] = department
                    self.next_run_times[(department, shift, frequency)] = start_time_dt 
                    schedule.every().hour.at(adjusted_start_time).do(self._execute_task, program, task)
                    next_hour = start_time_dt.replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)
                    start_time_dt = start_time_dt.replace(hour=next_hour.hour)
                next_occurrence, _, _ = self._get_next_occurrence_of_shift(self.shift_times[department]['Days'])
                while next_occurrence <= next_occurrence.replace(hour=7, minute=0):
                    adjusted_start_time = next_occurrence.strftime('%H:%M')
                    task['department'] = department
                    self.next_run_times[(department, shift, frequency)] = next_occurrence  
                    schedule.every().hour.at(adjusted_start_time).do(self._execute_task, program, task)
                    next_hour = next_occurrence.replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)
                    next_occurrence = next_occurrence.replace(hour=next_hour.hour)

            else:
                interval = (datetime.strptime(end_time, '%H:%M') - datetime.strptime(start_time, '%H:%M')) / 4

                for i in range(1, 5):
                    quarter_time = (datetime.strptime(start_time, '%H:%M') + i * interval).strftime('%H:%M')
                    next_run_time = datetime.now().replace(hour=int(quarter_time[:2]), minute=int(quarter_time[3:]), second=0, microsecond=0)
                    task['department'] = department
                    self.next_run_times[(department, shift, frequency)] = next_run_time  
                    schedule.every().day.at(quarter_time).do(self._execute_task, program, task)

                next_run_time = datetime.now().replace(hour=int(end_time[:2]), minute=int(end_time[3:]), second=0, microsecond=0)
                task['department'] = department
                self.next_run_times[(department, shift, frequency)] = next_run_time  
                schedule.every().day.at(end_time).do(self._execute_task, program, task)

    def _execute_task(self, program: callable, task: dict):
        '''
            Executes the program specified for the shift at the designated department
            
            Args:
                program (callable): The program/function to be executed.
                task (dict): The task dictionary containing the department, shift, and frequency.
        '''
        next_run_time = self.next_run_times.get((task['department'], task['shift'], task['frequency']), None)
        if next_run_time is None:
            return
        formatted_next_run_time = next_run_time.strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Running {task['frequency']} task for {task['department']} department, shift: {task['shift']} at {formatted_next_run_time}")
        try:
            program(task['department'], task['shift'])
        except Exception as e:
            logging.error(f"Error occurred during task execution: {e}")
    
    def run(self):
        while True:
            for (department, shift), tasks in self.tasks.items():
                print(tasks)
                current_time = datetime.now()
                next_run_time = self.next_run_times.get((department, shift), None)
                if current_time >= next_run_time:
                    self._execute_task(department, shift)
                    self.next_run_times[(department, shift)] = self._get_next_run_time(
                        self.tasks[(department, shift)], department, shift
                    )
            time.sleep(60)

    def clear_console(self):
        """
        Clear the console based on the OS.
        """
        os.system('cls' if os.name == 'nt' else 'clear')

    def stop(self):
        """
        Stop the scheduler.
        """
        schedule.clear()

    def _get_next_run_time(self, task: str, department: str, shift: str)-> datetime:
        '''
            Calculates the next run time for the shift based on the start times
            and end times provided in the shift_times.json file along with the fequency provided.
            
            Args:
                task (str): To get the frequency requested i.e, hourly, quarterly or daily.
                department (str): The department to run the bat file for.
                shift (str): The shift to run the bat file for.
            
            Returns:
                datetime: The next run time for the shift.
                
        '''
        shift_time = self.shift_times.get(department, {}).get(shift)
        if not shift_time:
            raise ValueError("Shift name not found in shift_times or missing 'start_time'")
        start_time = parse(shift_time["start_time"])
        end_time = parse(shift_time["end_time"])
        current_time = datetime.now()
        next_run_time = current_time.replace(hour=start_time.hour, minute=start_time.minute)
        if shift == "Days":
            if next_run_time.hour >= end_time.hour:
                next_run_time += timedelta(days=1)
                next_run_time = next_run_time.replace(hour=start_time.hour, minute=start_time.minute) 
        if shift == "Nights" and next_run_time.hour <= 9:
            next_run_time += timedelta(days=1)
            next_run_time = next_run_time.replace(hour=start_time.hour, minute=start_time.minute)
        frequency = task.get('frequency')
        if frequency == 'hourly':
            min = 60  
        elif frequency == 'daily':
            min = 1440  
        elif frequency == 'quarterly':
            if shift == "Days":
                shift_duration = (end_time - start_time).total_seconds() / 60
                min = shift_duration / 4
            else:
                shift_duration = (start_time- end_time).total_seconds() / 60
                min = shift_duration / 4
        else:
            min = 1440  

        while next_run_time <= current_time:
            next_run_time += timedelta(minutes=int(min))

            if shift == "Nights" and next_run_time.hour >= 24:
                next_run_time += timedelta(days=1)
                next_run_time = next_run_time.replace(hour=start_time.hour, minute=start_time.minute)

        self.next_run_times[(department, shift)] = next_run_time
        return next_run_time

    def clear_console(self):
        """
        Clear the console based on the OS.
        """
        os.system('cls' if os.name == 'nt' else 'clear')

    def display_animation(self, frames=frames, repeat=True, frame_delay=0.5):
        """
        Display the animation in the console.
        """
        task = self.view_scheduled_tasks()
        title = '''
 _____         _      ____       _              _       _           
|_   _|_ _ ___| | __ / ___|  ___| |__   ___  __| |_   _| | ___ _ __ 
  | |/ _` / __| |/ / \___ \ / __| '_ \ / _ \/ _` | | | | |/ _ \ '__|
  | | (_| \__ \   <   ___) | (__| | | |  __/ (_| | |_| | |  __/ |   
  |_|\__,_|___/_|\_\ |____/ \___|_| |_|\___|\__,_|\__,_|_|\___|_|   
        '''
        title = "\n".join([" " * (spacing-8)  + line for line in title.split("\n")])
        scheduled_jobs = schedule.get_jobs()
        while True:
            for frame in frames:
                self.clear_console()
                print(f'{title}\n{frame}\n\n{task}')
                time.sleep(frame_delay)
            if not repeat:
                break

def execute_task(department: str = None, shift: str = None, bat_file: str = None):
    '''
        Executess the bat_file specified for the shift at the designated department
        
        Args:
            department (str): The department to run the bat file for.
            shift (str): The shift to run the bat file for.
            bat_file (str): The bat file to run (looking in Desktop\Bat Files).
    '''
    bat_file = bat_folder + r'\{}.bat'.format(bat_file)
    if bat_file == None:
        bat_file = f'{department}_{shift}'
    elif department == None:
        department = input('Please enter the department: ').capitalize()
    elif shift == None:
        shift = input('Please enter Days or Nights: ')   
    elif not os.path.exists(bat_file):
        print(f'bat file path not found \n\t {bat_file}')
        python_file_path = input('Where is your python file located?: ')
        make_bat_files(bat_file_name= bat_file, python_file_path = python_file_path)
    else:
        try: 
            call([bat_file])
            logging.info(f"Running {bat_file} for {department} department, shift: {shift} at {time.strftime('%H:%M')}")
        except:
            logging.error(f"Failed to run {bat_file} for {department} department, shift: {shift} at {time.strftime('%H:%M')}")
            
    return bat_file.split('\\')[-1].split('.')[0]
        
def load_shift_times(json_file: str)-> str:
    """
    Load shift times from a JSON file.

    Args:
        json_file (str): The path to the JSON file containing shift details.

    Returns:
        dict: A dictionary with shift details.
    """
    with open(json_file, 'r') as file:
        shift_times = json.load(file)
    return shift_times

def make_bat_folder():
    '''
        Create Bat file in user Desktop if one does not exist.   
    '''
    if not os.path.exists(Desktop+r'\Bat Files'):
        os.makedirs(Desktop+r'\Bat Files')

def display_link(url: str, center:boolean = False) -> str:
    '''
        Creates a url blue clickable link.
        
        Args:
            url (str): The url you want to make clickable.
            center (boolean): If set to true, the url is centered.
        
        Returns:
            str: A clickable url. 
    '''
    colorama.init()
    clickable_link = f"{Fore.BLUE}{url}{Style.RESET_ALL}"
    if center:
        clickable_link = clickable_link.center(terminal_width)
    return clickable_link

def correct_file_path(file_path:str)-> str:
    '''
        Corrects file path to be compatible with python.
        
        Args:
            file_path (str): The file path to correct.
        
        Returns:
            str: The corrected file path.
    '''
    file_path = file_path.replace('\\', '/')
    return file_path
        
def make_bat_files(bat_file_name: str = None, python_file_path: str = None):
    '''
        Help make bat files to execute python modules.
        
        Args:
            bat_file_name (str): The name of the bat file to create.
            python_file_path (str): The path to the python file to execute.

    '''
    make_bat_folder()
    if python_file_path == None:
        python_file_path = input('Where is your python file located?: ')
    #testing
    python_file_path = Desktop + r'/'
    if not os.path.exists(python_file_path):
        raise FileNotFoundError(f'File not found at {python_file_path}')
    else:
        if bat_file_name == None:
            bat_file_name = input('What would you like to name the file? ')
        if bat_file_name.split('.')[-1] != 'bat':
            bat_file_name = bat_file_name.split('.')[0] + '.bat'
    try:
        python_exe = sys.executable
    except:
        link = display_link('https://github.com/wiljdaws/scheduler/wiki')
        raise FileNotFoundError(f'Sorry, {user} \n\tThe Python executable was not found. Please install python.\n\t If you are utilizing this program at Amazon Python can be installed from software center.\n\t If you need further instruction follow my wiki at {link}')
    path_to_bat = Desktop + r'\Bat Files'
    try:
        with open(path_to_bat+r'\{}'.format(bat_file_name), 'w') as file:
            file.write(f'\"{python_exe}\" \"{python_file_path}\"')
    except SyntaxError as e:
        print(f"Syntax Error {e}")
        # flip \ to / in file
        file = correct_file_path(file)
        with open(path_to_bat+r'\{}'.format(bat_file_name), 'w') as file:
            file.write(f'\"{python_exe}\" \"{python_file_path}\"')

def user_menu():
    '''
        Create a ordered list user menu to execute commands
        
    '''
    with open('shift_times.json', 'r') as file:
        shift_times = json.load(file)
    scheduler = TaskScheduler(shift_times)
    wiki = display_link('https://github.com/wiljdaws/scheduler/wiki')
    print(f'Hello {user}, thank you for utilizing Task Scheduler! Created by wiljdaws\n\n If you would like help with the getting started please refer to {wiki}\n\n')
    print('='* (terminal_width//2))
    menu_options = ['Create bat file', 'Schedule a task', 'Start scheduler', 'Quit          ']
    while True:
        print()
        print('USER MENU\n'.center(terminal_width//2))
        for options in menu_options:
            print(f'{menu_options.index(options)+1}. {options}'.center(terminal_width//2))
        print('\n'+'='* (terminal_width//2))
        user_input = input('Please enter the number of the method you would like to run: ')
        if user_input == '1':
            make_bat_files()
        elif user_input == '2':
            department = input('Which department are you scheduling a task for?: ')
            shift = input('Which shift (Days or Nights) are you sheduling a task for?: ')
            frequency = input('What is the frequency of the task (hourly, daily, or quarterly)?: ')
            bat_file = input('What is the name of the bat file?: ')
            line = f'{department},{shift},{frequency},{bat_file}\n'
            if not os.path.exists('scheduled_tasks.csv'):
                with open('scheduled_tasks.csv', 'w') as file:
                    file.write('department,shift,frequency,bat_file\n')
                    file.write(line)
            else:
                with open('scheduled_tasks.csv', 'a') as file:
                    file.write(line)
        elif user_input == '3':
            scheduler.display_animation()       
            scheduler.run()
        elif user_input.lower() == '4' or 'q':
            break
        else:
            raise ValueError('Please enter 1, 2, 3, 4 or q')
        
if __name__ == "__main__":
    # user_menu()
     # Load shift times from JSON file
    with open('shift_times.json', 'r') as file:
        shift_times = json.load(file)

    # Create an instance of TaskScheduler
    scheduler = TaskScheduler(shift_times)

    # Add tasks for specific departments and shifts
    scheduler.add_task_for_department_shift(department='Inbound', shift='Days', frequency='hourly', program=execute_task('Inbound', 'Days', 'iol_test_test'))
    scheduler.add_task_for_department_shift(department='Outbound', shift='Days', frequency='hourly', program=execute_task('Outbound', 'Days', 'money_tree'))
    # View the scheduled tasks
    print(scheduler.view_scheduled_tasks())

    scheduler.display_animation()

    # Start the scheduler
    scheduler.run()