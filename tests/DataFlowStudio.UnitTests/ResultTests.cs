using Nexus.Primitives;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

public sealed class ResultTests
{
    [Fact]
    public void Success_is_successful_and_carries_no_error()
    {
        var result = Result.Success();

        result.IsSuccess.ShouldBeTrue();
        result.IsFailure.ShouldBeFalse();
        result.Error.ShouldBe(Error.None);
    }

    [Fact]
    public void Failure_carries_the_error()
    {
        var error = Error.Validation("bad input");

        Result result = error; // implicit conversion

        result.IsFailure.ShouldBeTrue();
        result.Error.ShouldBe(error);
    }

    [Fact]
    public void Generic_success_exposes_the_value()
    {
        Result<int> result = 42; // implicit conversion

        result.IsSuccess.ShouldBeTrue();
        result.Value.ShouldBe(42);
    }

    [Fact]
    public void Accessing_the_value_of_a_failure_throws()
    {
        Result<int> result = Error.NotFound("missing");

        var act = () => result.Value;

        Should.Throw<InvalidOperationException>(() => act());
    }

    [Theory]
    [InlineData("validation")]
    [InlineData("not_found")]
    [InlineData("conflict")]
    public void Error_factories_set_the_code(string expectedCode)
    {
        var error = expectedCode switch
        {
            "validation" => Error.Validation("x"),
            "not_found" => Error.NotFound("x"),
            _ => Error.Conflict("x"),
        };

        error.Code.ShouldBe(expectedCode);
    }
}
