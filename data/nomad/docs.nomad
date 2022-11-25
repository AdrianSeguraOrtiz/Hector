job "docs" {
  datacenters = ["dc1"]

  group "example" {
    task "server" {
      driver = "docker"
      config {
        image = "hashicorp/http-echo"
        args  = ["-text", "hello"]
      }

      resources {
        memory = 128
      }
    }
  }
}