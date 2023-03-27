from pathlib import Path
import typer

def concat_files(
    input_file_1: str = typer.Option(
        ..., help="Input file 1"
    ),
    input_file_2: str = typer.Option(
        ..., help="Input file 2"
    ),
    output_file: str = typer.Option(
        "output.txt", help="Output file"
    )
):

    f = open(input_file_1, "r")
    message_1 = f.read()
    f.close()

    f = open(input_file_2, "r")
    message_2 = f.read()
    f.close()

    concat = message_1 + " " + message_2
    print(concat)

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    f = open(output_file, "a")
    f.write(concat)
    f.close()

if __name__ == "__main__":
    typer.run(concat_files)

# cd data/hector/toy_components/concat_files
# docker build -t adriansegura99/dag_kubernetes_concat-files .
# docker push adriansegura99/dag_kubernetes_concat-files
# cd ../../../../