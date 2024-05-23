import csv

class SimpleDataReader:
    def __init__(self):
        '''
        Initialize the SimpleDataReader object.
        '''
        self.column_to_index = {}
        self.buffer = []

    def read_data_csv(self, dataset, path, offset=0, size=None, header=False):
        '''
        Read the data from a CSV file with specified offset and size of data to be read.
        
        Args:
            dataset (str): The name of the dataset.
            path (str): The path to the directory containing the CSV file.
            offset (int): The number of rows to skip at the beginning of the file.
            size (int, optional): The number of rows to read. If None, all rows are read.
            header (bool, optional): Whether the CSV file has a header row.

        Returns:
            list: A list containing the read data.
        '''
        file_path = f'{path}/{dataset}.csv'
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            #Create a dictionary that is useful for mapping to an index
            if header:
                header = next(reader)
                self.column_to_index = {column_name: index for index, column_name in enumerate(header)}
            #Skip lines until we reach offset
            for _ in range(offset):
                next(reader)
            # Read the specified number of rows if size is provided, else read all rows
            if size is not None:
                data = [next(reader) for _ in range(size)]
            else:
                data = [row for row in reader]
        self.buffer = data
        return data

    def column(self, column_name):
        '''
        Extract column from the read data using the specified column name.

        Args:
            column_name (str): The name of the column to extract.

        Returns:
            list: A list containing the extracted columns.
        '''
        #Extract the index
        index = self.column_to_index[column_name]
        extracted_column = []
        #Extract columns
        for line in self.buffer:
             extracted_column.append(line[index])
        return  extracted_column
