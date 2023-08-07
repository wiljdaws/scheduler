
# Task Scheduling Application

![Task Scheduling Application Logo](https://github.com/wiljdaws/scheduler/assets/98637668/b6ecff93-b244-4ee9-8c0d-3ad069829ffc)

Organize your tasks efficiently with the Task Scheduling Application. This application enables you to schedule tasks based on different shifts and frequencies, ensuring effective time management and streamlined operations.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
  - [Installation](#installation)
  - [Usage](#usage)
- [Configuration](#configuration)
- [Scheduled Tasks](#scheduled-tasks)
- [Contributing](#contributing)
- [License](#license)

## Features

- Schedule tasks for various departments and shifts.
- Specify task frequencies as hourly, daily, weekly, or quarterly.
- Intuitive visualization of the scheduled tasks through a tabulated interface.
- Automatic task execution based on the defined schedule.

## Getting Started

### Installation

1. Clone the repository:

   ```sh
   git clone https://github.com/wiljdaws/scheduler.git
   cd scheduler
   ```

2. Install the required dependencies:

   ```sh
   pip install -r requirements.txt
   ```

### Usage

1. Customize Shift Times:
   - Update the `shift_times.json` file with the desired shift start and end times for different departments and shifts.

2. Run the application:

   ```sh
   python main.py
   ```

## Configuration

The `shift_times.json` file contains the configuration for different shifts and their corresponding start and end times. Modify this file to suit your organization's scheduling needs.

## Scheduled Tasks

Easily schedule and manage tasks using the Task Scheduling Application:

1. **Schedule a Task:**
   - Open the `main.py` file.
   - Create an instance of `TaskScheduler`.
   - Use the `schedule_task` method to schedule a task for a specific department and shift.

2. **View Scheduled Tasks:**
   - Run the application using `python main.py`.
   - The application will display the list of scheduled tasks in a clear tabulated format.

## Contributing

We welcome contributions from the community! If you have any ideas, suggestions, or improvements, please feel free to submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Enhance your task management with the Task Scheduling Application. Its user-friendly interface and efficient scheduling capabilities make it an ideal choice for businesses and individuals seeking to optimize their workflows.

For more information and updates, contact us at williamsjdawson@gmail.com.

