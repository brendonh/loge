package main

type Person struct {
	Name string
	Age uint32
	Bits []uint16 `loge:"copy"`
}

type Pet struct {
	Name string
	Species string
}

func main() {
	LinkSandbox()
	//WriteBench()
	//Sandbox()
	//Example()
}