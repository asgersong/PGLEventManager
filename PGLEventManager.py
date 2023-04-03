from threading import Event
import keyboard

import PGLEventManagerModel
import PGLEventManagerController


def main():
    stop_daemon = Event()
    print("Press 'x' to terminate")

    model = PGLEventManagerModel("localhost", "PGL", "PGL", "PGL")
    controller = PGLEventManagerController("localhost", model)

    controller.startListening()

    while not stop_daemon.is_set():
        if keyboard.is_pressed('x'):
            stop_daemon.set()

    print("Exiting")
    controller.stopListening()


if __name__ == "__main__":
    main()
