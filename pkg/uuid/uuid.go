package uuid

import "github.com/google/uuid"

func V7() uuid.UUID {
	return uuid.Must(uuid.NewV7())
}
