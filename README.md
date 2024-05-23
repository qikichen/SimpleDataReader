# SimpleDataReader
SimpleDataReader: A Simple Datareader for files that can be used for any kind of project. The project was made during another project, where I was tasked to read/load data and it is something that comes up many times in multiple projects. I decided to make a GitHub repo of my DataReader with the intention of using it for any future project for which I need to read/load data. So far it allows you to read a specified number of rows with options for skipping an offset and handling headers. You can also extract specific columns by name. Currently in use for benchmarking purposes and TVDGraphDB. Future versions will support additional file formats. 

## Features

- Read data from CSV files with options for offset and size
- Handle CSV files with or without headers
- Extract specific columns by name

## Installation

Simply clone the repository and use the provided class in your project.

```bash
git clone https://github.com/qikichen/SimpleDataReader.git
```

## Usage
Here’s a basic example of how to use SimpleDataReader:

```python
from SimpleDataReader import SimpleDataReader

# Initialize the data reader
reader = SimpleDataReader()

# Read data from a CSV file
data = reader.read_data_csv(dataset='example_dataset', path='path/to/csv', offset=0, size=10, header=True)

# Extract a specific column by name
column_data = reader.column(column_name='example_column')

print(data)
print(column_data)
```

## Future Plans
- Support for additional file formats such as JSON, XML, and Excel.
- Advanced data processing capabilities.

## Contributing
Contributions are welcome! Please fork the repository and submit pull requests for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.

