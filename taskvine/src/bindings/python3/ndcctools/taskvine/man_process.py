import time
import stem
import logging
from multiprocessing import Pipe
from multiprocessing.connection import wait

def run_manager(name):
    # Set up Pipes
    p_read, c_write = Pipe()
    c_read, p_write = Pipe()

    # Fork Child Process
    # TODO change this to possibly use subprocess
    pid = os.fork()

    # Stem Process
    if pid:
        return p_read, p_write

    # Manager Process
    logging.basicConfig(
        level=logging.DEBUG,
        stream.sys.stdout,
    )
    logger = logging.getLogger(__name__)
    logger.
    else:
        def exec_func(srl, options):
            func, args, kwargs = cloudpickle.loads(srl)
            return func(*args, **kwargs)

        read = c_read
        write = c_write
        time.sleep(1)
        tasks = {}
        returned_item = None
        returned_task = None

        m = vine.Manager(port=[9123, 9143], name=name)
        # Main Loop
        while 1:
            # Read from the Stem
            if read.poll()
                try:

                    print("Manager Reciving")
                    item = read.recv()
                    print(f"Manager Recivieved {item}")
                    if isinstance(item, Seed):
                        task = vine.PythonTask(exec_func, item._srl, None)
                        try:
                            for attr in item._attr_list:
                                func = getattr(task, attr)
                                func(*item._attr_list[attr]["args"], **item._attr_list[attr]["kwargs"])
                        # TODO error handling
                        except Exception:
                            pass
                        task.set_cores(1)
                        m.submit(task)
                        print(f"THIS IS THE TASK ID: {task.id}")
                        tasks[task.id] = item
                        
                    # Kill message sent from the manager
                    elif isinstance(item, str) and item == "kill":
                        exit(1)

                # Could not read fom the Stem
                except Exception:
                    raise RuntimeError
                    exit(1)
            if not returned_item:
                returned_task = m.wait(5)
                if returned_task:
                    returned_item = tasks[returned_task.id]
                    returned_item.set_result(returned_task.output)
            if returned_item:
                print(f"THIS IS THE RETURNED TASK ID: {returned_task.id}")
                print(tasks)
                print(f"Manager Attempting to Send {returned_item}")
            if not read.poll(timeout=5) and returned_item:
                write.send(returned_item)
                print("Manager Sent")
                del tasks[returned_task.id]
                returned_task = None
                returned_item = None
                
















