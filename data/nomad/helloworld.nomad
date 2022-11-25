job "Hello-World-Job" {
  type = "batch"
  datacenters = ["dc1"]

  group "Hello-World-Task-Group" {
    task "Hello-World-Task" {
      driver = "docker"
      config {
        image = "ubuntu"
        command = "echo"
        args  = ["hello world"]
      }
    }
  }
}