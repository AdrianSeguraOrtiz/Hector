from pathlib import Path
import typer

def count_letters(
    input_file: str = typer.Option(
        ..., help="Input file with the message"
    ),
    output_file: str = typer.Option(
        "output.txt", help="Output file"
    ),
):

    f = open(input_file, "r")
    message = f.read()
    num_letters = len(message)
    f.close()

    result = "The number of letters is: " + str(num_letters)
    print(result)

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    f = open(output_file, "a")
    f.write(result)
    f.close()

if __name__ == "__main__":
    typer.run(count_letters)

# cd data/hector/toy_components/count_letters
# docker build -t adriansegura99/dag_kubernetes_count-letters .
# docker push adriansegura99/dag_kubernetes_count-letters
# cd ../../../../