import os
import json
from reedsolo import RSCodec

class FileSplitter:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_size = os.path.getsize(file_path)
        self.chunk_size = self.calculate_chunk_size()
        self.num_data_chunks = self.calculate_num_data_chunks()
        self.num_parity_chunks = self.calculate_num_parity_chunks()
        self.rs_codec = RSCodec(self.num_parity_chunks)

    def calculate_chunk_size(self):
        if self.file_size < 1024 * 1024:  
            return 64 * 1024
        elif self.file_size < 5 * 1024 * 1024:  
            return 128 * 1024
        else:
            return 256 * 1024

    def calculate_num_data_chunks(self):
        if self.file_size < 1024 * 1024:  
            return 2
        elif self.file_size < 5 * 1024 * 1024:  
            return 4
        else:
            return 8

    def calculate_num_parity_chunks(self):
        if self.file_size < 1024 * 1024:  
            return 1
        elif self.file_size < 5 * 1024 * 1024:  
            return 2
        else:
            return 4

    def split_file(self, output_dir):
        try:
            chunks = []
            with open(self.file_path, 'rb') as file:
                while True:
                    chunk = file.read(self.chunk_size)
                    if not chunk:
                        break
                    chunks.append(chunk)

            num_chunks = len(chunks)
            if num_chunks % self.num_data_chunks != 0:
                padding_size = self.num_data_chunks - (num_chunks % self.num_data_chunks)
                chunks.extend([b'\x00' * self.chunk_size] * padding_size)

            encoded_chunks = []
            for i in range(0, len(chunks), self.num_data_chunks):
                data_chunks = chunks[i:i + self.num_data_chunks]
                data = b''.join(data_chunks)
                encoded_data = self.rs_codec.encode(list(data))
                encoded_chunks.append(bytes(encoded_data))

            chunk_metadata = []
            for i, chunk in enumerate(encoded_chunks):
                file_name = f"chunk_{i}.dat"
                with open(os.path.join(output_dir, file_name), 'wb') as file:
                    file.write(chunk)
                chunk_metadata.append({
                    'file_name': file_name,
                    'chunk_id': i
                })

            with open(os.path.join(output_dir, 'chunk_metadata.json'), 'w') as metadata_file:
                json.dump({
                    'chunk_size': self.chunk_size,
                    'num_data_chunks': self.num_data_chunks,
                    'num_parity_chunks': self.num_parity_chunks,
                    'chunks': chunk_metadata
                }, metadata_file)
        except Exception as e:
            print(f"An error occurred: {e}")

def validate_file_path(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError("The file was not found.")
    if not os.path.isfile(file_path):
        raise ValueError("The path is not a file.")

def validate_output_dir(output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if not os.path.isdir(output_dir):
        raise ValueError("The output path is not a directory.")

if __name__ == "__main__":
    try:
        file_path = input("Enter the file path: ")
        validate_file_path(file_path)
        output_dir = input("Enter the output directory: ")
        validate_output_dir(output_dir)
        file_splitter = FileSplitter(file_path)
        print(f"Chunk size: {file_splitter.chunk_size}")
        print(f"Number of data chunks: {file_splitter.num_data_chunks}")
        print(f"Number of parity chunks: {file_splitter.num_parity_chunks}")
        file_splitter.split_file(output_dir)
    except Exception as e:
        print(f"An error occurred: {e}")
