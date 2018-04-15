defmodule HospitalTest do
  use ExUnit.Case
  doctest Hospital

  test "greets the world" do
    assert Hospital.hello() == :world
  end
end
