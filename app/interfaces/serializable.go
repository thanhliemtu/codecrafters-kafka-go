package interfaces

import "log"

type Serializable interface {
	Serialize() ([]byte, error)
}

func PreviewMessage(m Serializable) {
	data, err := m.Serialize()
	if err != nil {
		log.Printf("Error serializing message: %v\n", err)
		return
	}
	log.Println("--- Message Preview ---")
	log.Printf("Struct Fields: %+v\n", m)
	log.Printf("Serialized Data:   [% x]\n", data)
	log.Println("-----------------------")
}
