package db

type Datasource struct {
	Host   string `yaml:"host" env:"DB_HOST" envDefault:"localhost"`
	Port   int    `yaml:"port" env:"DB_PORT" envDefault:"5434"`
	User   string `yaml:"user" env:"DB_USER" envDefault:"postgres"`
	Pass   string `yaml:"pass" env:"DB_PASS" envDefault:"postgres"`
	Name   string `yaml:"name" env:"DB_NAME" envDefault:"postgres"`
	Schema string `yaml:"schema" env:"DB_SCHEMA" envDefault:"public"`
}
