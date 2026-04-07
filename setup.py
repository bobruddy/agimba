from setuptools import setup

setup(
    name='agimba-tools',
    version='1.0.0',
    py_modules=['import_csvs_to_sheets', 'percussion_volunteer_list'],
    install_requires=[
        'gspread',
        'google-auth',
        'python-dateutil',
        'pytz',
    ],
    entry_points={
        'console_scripts': [
            'import_csvs_to_sheets=import_csvs_to_sheets:main',
            'percussion_volunteer_list=percussion_volunteer_list:main',
        ],
    },
    author='Robert Ruddy',
    description='Import CSVs to Google Sheets with formatting and QA',
    license='GPL-3.0',
)
