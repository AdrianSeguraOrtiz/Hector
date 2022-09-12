import typer

def concat_messages(
    message_1: str = typer.Option(
        ..., help="Message 1"
    ),
    message_2: str = typer.Option(
        ..., help="Message 2"
    ),
    output_file: str = typer.Option(
        "output.txt", help="Output file"
    )
):

    f = open(output_file, "a")
    f.write(message_1 + " " + message_2)
    f.close()

if __name__ == "__main__":
    typer.run(concat_messages)

# cd data/kubernetes/toy_components/concat_messages
# docker build -t adriansegura99/dag_kubernetes_concat-messages .
# docker push adriansegura99/dag_kubernetes_concat-messages
# cd ../../../../