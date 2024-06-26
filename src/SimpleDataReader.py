import csv
import json
import os
import tarfile
import gzip
from typing import Any


class SimpleDataReader:
    def __init__(self) -> None:
        """
        Initialize the SimpleDataReader object.
        """
        self.column_to_index: dict[str, int] = {}
        self.buffer: list[Any] = []
        self.labels: list[str] = []

    def read_data_csv(
        self,
        dataset: str,
        path: str,
        offset: int = 0,
        size: int | None = None,
        header: bool = False,
    ) -> list[Any]:
        """
        Read the data from a CSV file with specified offset and size of data to be read.

        Args:
            dataset (str): The name of the dataset.
            path (str): The path to the directory containing the CSV file.
            offset (int): The number of rows to skip at the beginning of the file.
            size (int, optional): The number of rows to read. If None, all rows are read.
            header (bool, optional): Whether the CSV file has a header row.

        Returns:
            list: A list containing the read data.
        """
        file_path = f"{path}/{dataset}.csv"
        with open(file_path, "r", newline="") as csvfile:
            reader = csv.reader(csvfile)
            # Create a dictionary that is useful for mapping to an index
            if header:
                header_str = next(reader)
                self.column_to_index = {
                    column_name: index for index, column_name in enumerate(header_str)
                }
            # Skip lines until we reach offset
            for _ in range(offset):
                next(reader)
            # Read the specified number of rows if size is provided, else read all rows
            if size is not None:
                data = [next(reader) for _ in range(size)]
            else:
                data = [row for row in reader]
        self.buffer = data
        return self.buffer

    def read_data_json(
        self, dataset: str, path: str, offset: int = 0, size: int | None = None
    ) -> list[Any]:
        """
        Read data from a JSON file and store it in the buffer.

        Args:
        - dataset: The name of the dataset (without the .json extension)
        - path: The path to the directory containing the JSON file
        - offset: The starting index to read from (default is 0)
        - size: The number of records to read (default is None, meaning read all)

        Returns:
        - The data read from the JSON file, stored in the buffer
        """

        file_path = f"{path}/{dataset}.json"
        with open(file_path, "r") as file:
            data = json.load(file)

        # Get the keys (column names) from the first item in the data
        keys = list(data[0].keys())

        # Map each key to its index and store it in column_to_index
        for i in range(len(keys)):
            self.column_to_index[keys[i]] = i

        # Apply offset and size to slice the data
        if size is not None:
            end_index = offset + size
            self.buffer = data[offset:end_index]
        else:
            self.buffer = data[offset:]

        return self.buffer

    def read_data_tsv_gz(
        self,
        dataset: str,
        path: str,
        offset: int = 0,
        size: int | None = None,
        header: bool = False,
    ) -> list[Any]:
        """
        Read the data from a gzipped TSV file with specified offset and size of data to be read.

        Args:
            dataset (str): The name of the dataset.
            path (str): The path to the directory containing the TSV file.
            offset (int): The number of rows to skip at the beginning of the file.
            size (int, optional): The number of rows to read. If None, all rows are read.
            header (bool, optional): Whether the TSV file has a header row.

        Returns:
            list: A list containing the read data.
        """
        file_path = f"{path}/{dataset}.tsv.gz"
        with gzip.open(file_path, "rt") as tsvfile:
            reader = csv.reader(tsvfile, delimiter="\t")
            # Create a dictionary that is useful for mapping to an index
            if header:
                header_str = next(reader)
                self.column_to_index = {
                    column_name: index for index, column_name in enumerate(header_str)
                }
            # Skip lines until we reach offset
            for _ in range(offset):
                next(reader)
            # Read the specified number of rows if size is provided, else read all rows
            if size is not None:
                data = [next(reader) for _ in range(size)]
            else:
                data = [row for row in reader]
        self.buffer = data
        return self.buffer

    def column(self, column_name: str) -> list[Any]:
        """
        Extract column from the read data using the specified column name.

        Args:
            column_name (str): The name of the column to extract.

        Returns:
            list: A list containing the extracted columns.
        """
        # Extract the index
        index = self.column_to_index[column_name]
        extracted_column = []
        # Extract columns
        for line in self.buffer:
            extracted_column.append(line[index])
        return extracted_column

    def unzip_and_read(
        self,
        tar_path: str,
        dataset: str,
        extract_path: str = "extracted",
        file_type: str = "csv",
        offset: int = 0,
        size: int | None = None,
        header: bool = False,
    ) -> list[Any]:
        """
        Unzips a tar.gz file and reads the data from the extracted files.

        Args:
            tar_path (str): The path to the tar.gz file.
            extract_path (str): The path where the extracted files will be stored.
            dataset (str): The name of the dataset to read (without extension).
            file_type (str): The type of file to read ('csv', 'json', or 'tsv.gz').
            offset (int): The number of rows to skip at the beginning of the file.
            size (int, optional): The number of rows to read. If None, all rows are read.
            header (bool, optional): Whether the CSV/TSV file has a header row (applicable if file_type is 'csv' or 'tsv.gz').

        Returns:
            list: A list containing the read data.
        """
        if not os.path.exists(extract_path):
            os.makedirs(extract_path)

        # Extract the tar.gz file and read
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(path=extract_path)
        if file_type == "csv":
            return self.read_data_csv(dataset, extract_path, offset, size, header)
        elif file_type == "json":
            return self.read_data_json(dataset, extract_path, offset, size)
        elif file_type == "tsv.gz":
            return self.read_data_tsv_gz(dataset, extract_path, offset, size, header)
        else:
            raise ValueError("Unsupported file type. Use 'csv', 'json', or 'tsv.gz'.")

    def write_data_csv(
        self, data: list[list[Any]], output_path: str, header: list[str] | None = None
    ) -> None:
        """
        Write a 2D list of data to a CSV file.

        Args:
            data (list[list[Any]]): The 2D list of data to write.
            output_path (str): The path where the CSV file will be written.
            header (list[str], optional): The header row to write, if any.

        Returns:
            None
        """
        with open(output_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            if header:
                writer.writerow(header)
            writer.writerows(data)


# test the SimpleDataReader class
if __name__ == "__main__":
    reader = SimpleDataReader()
    data = reader.read_data_tsv_gz("name.basics", "imdb_data", size=500)
    reader.write_data_csv(data, "test_data/output.csv")
