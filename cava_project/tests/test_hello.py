from cava_project import hello


def test_hello():
    hello_text = hello.say_hello()

    assert hello_text == "Hello world."
