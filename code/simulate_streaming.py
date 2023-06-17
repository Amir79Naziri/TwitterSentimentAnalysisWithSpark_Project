import os
from time import sleep

if __name__ == "__main__":
    streaming_source_path = "../data/streaming_data"
    streaming_output_path = "../stream"
    print("Streaming ...")
    for streaming_source in os.listdir(streaming_source_path):
        if streaming_source.endswith(".csv"):
            with open(
                os.path.join(streaming_source_path, streaming_source),
                mode="r",
                encoding="utf-8",
                errors="ignore",
            ) as inp:
                for line in inp:
                    counter = 0
                    streaming_output = (
                        "tweet - "
                        + str(counter)
                        + "#"
                        + str(line.split(",")[1])
                        + ".csv"
                    )
                    counter += 1
                    with open(
                        os.path.join(streaming_output_path, streaming_output),
                        mode="w",
                        encoding="utf-8",
                    ) as out:
                        out.write(line)
                    sleep(1)
                    
