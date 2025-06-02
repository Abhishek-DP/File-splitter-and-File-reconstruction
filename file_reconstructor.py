import os
import json
from reedsolo import RSCodec

class FileReconstructor:
    def __init__(self, chunk_size, num_data_chunks, num_parity_chunks):
        self.chunk_size = chunk_size
        self.num_data_chunks = num_data_chunks
        self.num_parity_chunks = num_parity_chunks
        self.rs_codec = RSCodec(num_parity_chunks)

    def reconstruct_file(self, input_dir, output_file_path):
        try:
            with open(os.path.join(input_dir, 'chunk_metadata.json'), 'r') as metadata_file:
                chunk_metadata = json.load(metadata_file)

            chunks = []
            for chunk in chunk_metadata['chunks']:
                try:
                    with open(os.path.join(input_dir, chunk['file_name']), 'rb') as file:
                        chunks.append((chunk['chunk_id'], file.read()))
                except FileNotFoundError:
                    chunks.append((chunk['chunk_id'], None))

            chunks.sort(key=lambda x: x[0])

            encoded_chunks = [chunk[1] for chunk in chunks if chunk[1] is not None]

            decoded_data = b''
            for chunk in encoded_chunks:
                decoded_chunk = bytes(self.rs_codec.decode(list(chunk))[0])
                decoded_data += decoded_chunk

            reconstructed_chunks = [decoded_data[i:i + self.chunk_size] for i in range(0, len(decoded_data), self.chunk_size)]

            output_dir = os.path.dirname(output_file_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)

            with open(output_file_path, 'wb') as output_file:
                for chunk in reconstructed_chunks:
                    output_file.write(chunk)
        except Exception as e:
            print(f"An error occurred: {e}")

def validate_input_dir(input_dir):
    if not os.path.exists(input_dir):
        raise FileNotFoundError("The input directory was not found.")
    if not os.path.isdir(input_dir):
        raise ValueError("The input path is not a directory.")

def validate_output_file_path(output_file_path):
    output_dir = os.path.dirname(output_file_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

if __name__ == "__main__":
    try:
        input_dir = input("Enter the directory containing the chunked files: ")
        validate_input_dir(input_dir)
        output_file_path = input("Enter the output file path: ")
        validate_output_file_path(output_file_path)
        with open(os.path.join(input_dir, 'chunk_metadata.json'), 'r') as metadata_file:
            chunk_metadata = json.load(metadata_file)
        file_reconstructor = FileReconstructor(chunk_metadata['chunk_size'], chunk_metadata['num_data_chunks'], chunk_metadata['num_parity_chunks'])
        file_reconstructor.reconstruct_file(input_dir, output_file_path)
    except Exception as e:
        print(f"An error occurred: {e}")
