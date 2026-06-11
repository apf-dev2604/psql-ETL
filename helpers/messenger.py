class Messenger:
    def info(self, message: str) -> None:
        print(message, flush=True)

    def warn(self, message: str) -> None:
        print(f"[WARN] {message}", flush=True)

    def error(self, message: str) -> None:
        print(f"[ERROR] {message}", flush=True)
