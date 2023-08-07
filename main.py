from datetime import datetime, timedelta
import linecache
from operator import call
import os
import shutil
import traceback
from numpy import spacing
import schedule
import time
import json
import logging
from tabulate import tabulate
from dateutil.parser import parse
num = 20
# get terminal width
terminal_width,_ = shutil.get_terminal_size() 
# set  avariable for spacing based on terminal width
spacing = int((terminal_width-15)/4)-3 # minus three because of the
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


# Set up logging configuration
logging.basicConfig(filename='scheduler.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logging.info("Scheduler started")


class TaskScheduler:
    """
    A task scheduler for a data analyst to run programs for different departments based on their shift times.

    Attributes:
        tasks (dict): A dictionary to store tasks for each department and shift.
        shift_times (dict): A dictionary containing the shift times for different departments and shifts.
        next_run_times (dict): A dictionary to store the next run time for each task.

    Methods:
        add_task(department, shift, frequency, program, start_time, end_time): Add a task to the scheduler.
        remove_task(department, shift, frequency): Remove a task from the scheduler.
        view_scheduled_tasks(): View the currently scheduled tasks.
        run(): Start the scheduler to run the tasks.
        stop(): Stop the scheduler.
    """

    def __init__(self, shift_times: dict = None):
        self.tasks = {}
        self.shift_times = shift_times if shift_times is not None else {}
        self.next_run_times = {}  # Dictionary to store the next run time for each task

    def is_valid_time_range(self, start_time, end_time):
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
            # For night shifts, we allow end time to be after midnight (i.e., the next day)
            if start_time_obj < end_time_obj:
                return True
            # But for the case where end time is before midnight, we add a day to the end time
            next_day_end_time_obj = end_time_obj + timedelta(days=1)
            return next_day_end_time_obj >= start_time_obj
        return False
    
    def is_night_shift(self, shift):
        """
        Check if the given shift is a night shift.

        Args:
            shift (str): The shift to check.

        Returns:
            bool: True if it's a night shift, False otherwise.
        """
        return shift == 'Nights'
        
    def _get_next_occurrence_of_shift(self, shift_info):
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

        # Check if the current time is before the start time of the shift
        if now < start_time:
            return now, start_time, end_time

        # Check if the current time is between the start and end time of the shift
        elif start_time <= now <= end_time:
            return now, start_time, end_time

        # If the current time is after the end time of the shift, calculate the next shift occurrence for the next day
        else:
            next_day = now + timedelta(days=1)
            return next_day.replace(hour=start_time.hour, minute=start_time.minute), start_time, end_time
    
    def get_task_by_department_shift_frequency(self, department, shift, frequency):
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


    def add_task(self, department, shift, frequency, program, start_time, end_time):
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

        self.shift = shift  # Set the current shift for the task being added

        if frequency == 'daily':
            raise ValueError("Daily tasks are not supported in this version of the scheduler.")
        elif not self.is_valid_time_range(start_time, end_time):
            raise ValueError(f"End time should be after start time.\n\t department: {department}\t shift: {shift} \n\t start_time: {start_time}\t end_time: {end_time}")

        key = (department, shift)
        if key not in self.tasks:
            self.tasks[key] = []

        # Check for overlapping tasks
        for existing_task in self.tasks[key]:
            if existing_task['frequency'] == frequency and not (end_time <= existing_task['start_time'] or start_time >= existing_task['end_time']):
                raise ValueError("Task overlaps with an existing one.")

        self.tasks[key].append({'frequency': frequency, 'program': program, 'start_time': start_time, 'end_time': end_time})

    def add_task_for_department_shift(self, department, shift, frequency, program):
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
        if frequency not in ['hourly', 'daily', 'quarterly']:
            raise ValueError("Invalid frequency. Please choose from 'hourly', 'daily', or 'quarterly'.")

        key = (department, shift)
        if key not in self.tasks:
            self.tasks[key] = []

        shift_times = self.shift_times.get(department, {}).get(shift, {})
        start_time = shift_times.get('start_time')
        end_time = shift_times.get('end_time')

        if frequency == 'daily':
            raise ValueError("Daily tasks are not supported in this version of the scheduler.")
        elif shift == 'Days' and start_time >= end_time:
            raise ValueError(f"End time should be after start time.\n\t department: {department}\t shift: {shift} \n\t start_time: {start_time}\t end_time: {end_time}")
        elif shift == 'Nights' and start_time <= end_time:
            raise ValueError(f"Start time should be after end time.\n\t start_time: {start_time} end_time: {end_time}")

        # Check for overlapping tasks
        for existing_task in self.tasks[key]:
            if existing_task['frequency'] == frequency and not (end_time <= existing_task['start_time'] or start_time >= existing_task['end_time']):
                raise ValueError("Task overlaps with an existing one.")

        # Create the task dictionary with the required details
        task = {'department': department, 'shift': shift, 'frequency': frequency, 'program': program, 'start_time': start_time, 'end_time': end_time}

        self.tasks[key].append(task)

    def remove_task(self, department, shift, frequency):
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

    def view_scheduled_tasks(self):
        tasks_table = []
        for (department, shift), tasks in self.tasks.items():
            for task in tasks:
                frequency = task['frequency']
                program = task['program'].__name__
                start_time = task['start_time']
                end_time = task['end_time']

                # Get the next run time using the _get_next_run_time method
                next_run_time = self._get_next_run_time(task, department, shift)
                # Convert next run time to month/day/year hh:mm
                next_run_time = next_run_time.strftime('%m/%d/%Y %H:%M')

                headers = ["Department", "Shift", "Frequency", "Program", "Start Time", "End Time", "Next Run Time"]
                tasks_table.append([department, shift, frequency, program, start_time, end_time, next_run_time])

        return tabulate(tasks_table, headers=headers, tablefmt="grid")

    def _schedule_task(self, program, task):
        department = task['department']
        shift = task['shift']
        frequency = task['frequency']
        start_time = task['start_time']
        end_time = task['end_time']

        next_run_time = None
        next_occurrence, shift_start_time, shift_end_time = self._get_next_occurrence_of_shift(self.shift_times[department][shift])

        if frequency == 'hourly':
            if self.is_night_shift(shift):
                # For night shifts, handle the hourly interval carefully due to the shift spanning midnight.
                start_time_dt = datetime.strptime(start_time, '%H:%M')
                end_time_dt = datetime.strptime(end_time, '%H:%M')

                # Schedule tasks for each hour within the night shift until the end_time
                while start_time_dt <= end_time_dt:
                    adjusted_start_time = start_time_dt.strftime('%H:%M')
                    # Add the 'department' key to the task dictionary
                    task['department'] = department
                    self.next_run_times[(department, shift, frequency)] = start_time_dt  # Store the next run time in the dictionary
                    schedule.every().hour.at(adjusted_start_time).do(self._execute_task, program, task)

                    # Increment the start_time_dt by one hour while keeping it within a valid date range
                    next_hour = start_time_dt.replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)
                    start_time_dt = start_time_dt.replace(hour=next_hour.hour)

                # Calculate the next occurrence of the morning shift for the department
                next_occurrence, _, _ = self._get_next_occurrence_of_shift(self.shift_times[department]['Days'])
                # Schedule tasks for each hour within the morning shift on the next day
                while next_occurrence <= next_occurrence.replace(hour=7, minute=0):
                    adjusted_start_time = next_occurrence.strftime('%H:%M')
                    # Add the 'department' key to the task dictionary
                    task['department'] = department
                    self.next_run_times[(department, shift, frequency)] = next_occurrence  # Store the next run time in the dictionary
                    schedule.every().hour.at(adjusted_start_time).do(self._execute_task, program, task)

                    # Increment the next_occurrence by one hour while keeping it within a valid date range
                    next_hour = next_occurrence.replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)
                    next_occurrence = next_occurrence.replace(hour=next_hour.hour)

            else:
                # For shifts other than night shifts, handle hourly tasks as before
                interval = timedelta(hours=1)
                current_time = datetime.now().replace(second=0, microsecond=0)
                next_run_time = current_time + interval

                # Schedule tasks for each hour within the time range
                while next_run_time <= datetime.now().replace(hour=int(end_time[:2]), minute=int(end_time[3:]), second=0, microsecond=0):
                    if next_run_time >= datetime.now().replace(hour=int(start_time[:2]), minute=int(start_time[3:]), second=0, microsecond=0):
                        adjusted_start_time = next_run_time.strftime('%H:%M')
                        # Add the 'department' key to the task dictionary
                        task['department'] = department
                        self.next_run_times[(department, shift, frequency)] = next_run_time  # Store the next run time in the dictionary
                        schedule.every().hour.at(adjusted_start_time).do(self._execute_task, program, task)

                    next_run_time += interval
        elif frequency == 'daily':
            raise ValueError("Daily tasks are not supported in this version of the scheduler.")
        
        elif frequency == 'quarterly':
            if self.is_night_shift(shift):
                # For night shifts, handle the interval carefully due to the shift spanning midnight.
                start_time_dt = datetime.strptime(start_time, '%H:%M')
                end_time_dt = datetime.strptime(end_time, '%H:%M')

                # Schedule tasks for each hour within the night shift until the end_time
                while start_time_dt <= end_time_dt:
                    adjusted_start_time = start_time_dt.strftime('%H:%M')
                    # Add the 'department' key to the task dictionary
                    task['department'] = department
                    self.next_run_times[(department, shift, frequency)] = start_time_dt  # Store the next run time in the dictionary
                    schedule.every().hour.at(adjusted_start_time).do(self._execute_task, program, task)

                    # Increment the start_time_dt by one hour while keeping it within a valid date range
                    next_hour = start_time_dt.replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)
                    start_time_dt = start_time_dt.replace(hour=next_hour.hour)

                # Calculate the next occurrence of the morning shift for the department
                next_occurrence, _, _ = self._get_next_occurrence_of_shift(self.shift_times[department]['Days'])
                # Schedule tasks for each hour within the morning shift on the next day
                while next_occurrence <= next_occurrence.replace(hour=7, minute=0):
                    adjusted_start_time = next_occurrence.strftime('%H:%M')
                    # Add the 'department' key to the task dictionary
                    task['department'] = department
                    self.next_run_times[(department, shift, frequency)] = next_occurrence  # Store the next run time in the dictionary
                    schedule.every().hour.at(adjusted_start_time).do(self._execute_task, program, task)

                    # Increment the next_occurrence by one hour while keeping it within a valid date range
                    next_hour = next_occurrence.replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)
                    next_occurrence = next_occurrence.replace(hour=next_hour.hour)

            else:
                # For shifts other than night shifts, handle quarterly tasks as before
                interval = (datetime.strptime(end_time, '%H:%M') - datetime.strptime(start_time, '%H:%M')) / 4

                for i in range(1, 5):
                    quarter_time = (datetime.strptime(start_time, '%H:%M') + i * interval).strftime('%H:%M')
                    next_run_time = datetime.now().replace(hour=int(quarter_time[:2]), minute=int(quarter_time[3:]), second=0, microsecond=0)
                    # Add the 'department' key to the task dictionary
                    task['department'] = department
                    self.next_run_times[(department, shift, frequency)] = next_run_time  # Store the next run time in the dictionary
                    schedule.every().day.at(quarter_time).do(self._execute_task, program, task)

                next_run_time = datetime.now().replace(hour=int(end_time[:2]), minute=int(end_time[3:]), second=0, microsecond=0)
                # Add the 'department' key to the task dictionary
                task['department'] = department
                self.next_run_times[(department, shift, frequency)] = next_run_time  # Store the next run time in the dictionary
                schedule.every().day.at(end_time).do(self._execute_task, program, task)


    def _execute_task(self, program: callable, task: dict):
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
            # Iterate over the next_run_times dictionary
            for (department, shift), next_run_time in self.next_run_times.items():
                # Get the current time
                current_time = datetime.now()
                
                # Check if it's time to run the task
                if current_time >= next_run_time:
                    # Run the task
                    self._execute_task(department, shift)
                    
                    # Update the next_run_time for the task
                    self.next_run_times[(department, shift)] = self._get_next_run_time(
                        self.tasks[(department, shift)], department, shift
                    )

            # Sleep for a minute and check again
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

    def _get_next_run_time(self, task, department, shift):
        shift_time = self.shift_times.get(department, {}).get(shift)
        if not shift_time:
            raise ValueError("Shift name not found in shift_times or missing 'start_time'")

        start_time = parse(shift_time["start_time"])
        end_time = parse(shift_time["end_time"])
        current_time = datetime.now()
        next_run_time = current_time.replace(hour=start_time.hour, minute=start_time.minute)
        if shift == "Days":
            # if the current hour is greater than the next run time
            if next_run_time.hour >= end_time.hour:
                next_run_time += timedelta(days=1)
                next_run_time = next_run_time.replace(hour=start_time.hour, minute=start_time.minute) 
        # If the shift is "Nights" and the next run time is after midnight
        if shift == "Nights" and next_run_time.hour <= 9:
            next_run_time += timedelta(days=1)
            next_run_time = next_run_time.replace(hour=start_time.hour, minute=start_time.minute)
        # Handle the case when 'frequency' is 'hourly', 'daily', or not provided
        frequency = task.get('frequency')
        if frequency == 'hourly':
            min = 60  # Set default frequency to 60 minutes (1 hour)
        elif frequency == 'daily':
            min = 1440  # Set default frequency to 1440 minutes (1 day)
        elif frequency == 'quarterly':
            # Calculate the time difference between shift start and end time
            if shift == "Days":
                shift_duration = (end_time - start_time).total_seconds() / 60
                min = shift_duration / 4
            else:
                shift_duration = (start_time- end_time).total_seconds() / 60
                min = shift_duration / 4
        else:
            min = 1440  # Set default frequency to 1440 minutes (1 day) for any other case

        while next_run_time <= current_time:
            # Convert the frequency value to an integer before using it in timedelta
            next_run_time += timedelta(minutes=int(min))

            # If the shift is "Nights" and the next_run_time goes past midnight,
            # adjust it to the next day.
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
        # center multi line title based on terminal width
        title = "\n".join([" " * (spacing-8)  + line for line in title.split("\n")])
        while True:
            for frame in frames:
                self.clear_console()
                print(f'{title}\n{frame}\n\n{task}')
                time.sleep(frame_delay)
            if not repeat:
                break


# Custom tasks for inbound, outbound, receive, ICQA, and ops
def inbound_quarterly_task(department, shift):
    print(f"Running inbound quarterly task for {department} department, shift: {shift} at {time.strftime('%H:%M')}")
    # Your inbound quarterly task logic here


def inbound_daily_task(department, shift):
    print(f"Running inbound daily task for {department} department, shift: {shift} at {time.strftime('%H:%M')}")
    # Your inbound daily task logic here


def inbound_hourly_task(department, shift):
    print(f"Running inbound hourly task for {department} department, shift: {shift} at {time.strftime('%H:%M')}")
    path_to_bat = r'C:\Users\wiljdaws\Desktop\Bat Files'
    try: call([path_to_bat+r'\weekly_hr.bat'])
    except: print("an error occured with the money tree workflow at:", time.time())
    # log statement


def load_shift_times(json_file):
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


if __name__ == "__main__":
    # Load shift times from JSON file
    with open('shift_times.json', 'r') as file:
        shift_times = json.load(file)

    # Create an instance of TaskScheduler
    scheduler = TaskScheduler(shift_times)

    # Add tasks for specific departments and shifts
    scheduler.add_task_for_department_shift(department='Inbound', shift='Days', frequency='hourly', program=inbound_hourly_task)
    scheduler.add_task_for_department_shift(department='Inbound', shift='Days', frequency='quarterly', program=inbound_quarterly_task)
    scheduler.add_task_for_department_shift(department='Outbound', shift='Nights', frequency='quarterly', program=inbound_quarterly_task)

    # View the scheduled tasks
    print(scheduler.view_scheduled_tasks())
    
    scheduler.display_animation()

    # Start the scheduler
    scheduler.run()