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
    num_letters = message.count()
    f.close()

    result = "The number of letters is: " + num_letters
    print(result)

    f = open(output_file, "a")
    f.write(result)
    f.close()

if __name__ == "__main__":
    typer.run(count_letters)

# cd data/hector/toy_components/count_letters
# docker build -t adriansegura99/dag_kubernetes_count-letters .
# docker push adriansegura99/dag_kubernetes_count-letters
# cd ../../../../