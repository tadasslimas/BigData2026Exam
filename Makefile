OS := $(shell uname)

# Parenkame OS specifinį failą
ifeq ($(OS), Darwin)
    COMPOSE_FILE := docker-compose.macos.yml
    HW_ENV       := environment.macos.env
else
    COMPOSE_FILE := docker-compose.linux.yml
    HW_ENV       := environment.linux.env
endif

# „Docker Compose“ leidžia įkelti kelis failus iš eilės.
# Vėliau nurodytas failas perrašo (override) anksčiau nurodytą.
up:
	@echo "--- Paleidžiama sistema ---"
	@echo "Naudojami nustatymai: Common + $(HW_ENV)"
	docker compose \
		--env-file environment.common.env \
		--env-file $(HW_ENV) \
		-f $(COMPOSE_FILE) \
		up --build

down:
	docker compose --env-file environment.common.env --env-file $(HW_ENV) -f $(COMPOSE_FILE) down


