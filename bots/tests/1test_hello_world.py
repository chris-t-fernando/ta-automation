import pytest
import hello_world


# @pytest.fixture(params=["int", "float", "str"])
@pytest.fixture(params=["int", "float", "str"])
def generate_initial_transform_parameters(request, mocker):
    if request.param == "int":
        test_input = 1, 2
        expected_output = 3
    elif request.param == "float":
        test_input = 1.5, 2.5
        expected_output = 4
    elif request.param == "str":
        test_input = 1, 2
        expected_output = 3

    mocker.patch.object(hello_world, "banana")
    mocker.hello_world.return_value("apple")

    return test_input, expected_output


def test_initial_transform(generate_initial_transform_parameters):
    test_input_one = generate_initial_transform_parameters[0][0]
    test_input_two = generate_initial_transform_parameters[0][1]
    expected_output = generate_initial_transform_parameters[1]
    assert hello_world.add(test_input_one, test_input_two) == expected_output
