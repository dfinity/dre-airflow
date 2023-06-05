from st2common.runners.base_action import Action


class MyEchoAction(Action):
    def run(self, echoable_text):
        print(echoable_text)

        if echoable_text == "sunny":
            return (True, {"weather": "sunny indeed"})
        return (True, {"weather": "stormy"})
