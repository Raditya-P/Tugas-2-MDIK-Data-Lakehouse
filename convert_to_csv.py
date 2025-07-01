import os
import csv
import re

def main():
    """
    Reads the raw text files from the aclImdb dataset, processes them in chunks,
    and writes the structured data to a single CSV file.
    """
    # The name of the output CSV file
    output_csv_file = 'reviews.csv'
    
    # The base directory where the 'test' and 'train' folders are located
    base_data_path = 'unstructuredmovie'

    print(f"Starting conversion... Output will be saved to '{output_csv_file}'")

    try:
        with open(output_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            # Define the CSV header
            fieldnames = ['review_id', 'rating', 'sentiment', 'dataset', 'review_text']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write the header row to the CSV file
            writer.writeheader()
            
            total_files_processed = 0

            # List of all directories to process
            paths_to_process = [
                (os.path.join(base_data_path, "train", "pos"), "train", "positive"),
                (os.path.join(base_data_path, "train", "neg"), "train", "negative"),
                (os.path.join(base_data_path, "test", "pos"), "test", "positive"),
                (os.path.join(base_data_path, "test", "neg"), "test", "negative")
            ]

            for path, dataset, sentiment in paths_to_process:
                print(f"--- Processing directory: {path} ---")
                
                if not os.path.isdir(path):
                    print(f"Warning: Directory not found, skipping: {path}")
                    continue

                files_in_dir = os.listdir(path)
                
                for filename in files_in_dir:
                    if filename.endswith(".txt"):
                        try:
                            # Extract ID and rating from filename like "123_9.txt"
                            match = re.match(r'(\d+)_(\d+)\.txt', filename)
                            if not match:
                                continue
                                
                            review_id, rating = match.groups()
                            
                            # Read the full text content of the review file
                            with open(os.path.join(path, filename), 'r', encoding='utf-8') as f:
                                review_text = f.read()

                            # Write the structured row to the CSV file
                            writer.writerow({
                                'review_id': review_id,
                                'rating': rating,
                                'sentiment': sentiment,
                                'dataset': dataset,
                                'review_text': review_text
                            })
                            
                            total_files_processed += 1

                        except Exception as e:
                            print(f"Error processing file {filename}: {e}")
                
                print(f"Finished processing directory. Total files so far: {total_files_processed}")

        print(f"\n--- Conversion complete! ---")
        print(f"Successfully processed {total_files_processed} files.")
        print(f"Data saved to '{output_csv_file}'.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    main()
