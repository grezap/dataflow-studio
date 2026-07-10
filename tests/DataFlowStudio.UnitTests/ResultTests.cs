using DataFlowStudio.SharedKernel;
using FluentAssertions;
using Xunit;

namespace DataFlowStudio.UnitTests;

public sealed class ResultTests
{
    [Fact]
    public void Success_is_successful_and_carries_no_error()
    {
        var result = Result.Success();

        result.IsSuccess.Should().BeTrue();
        result.IsFailure.Should().BeFalse();
        result.Error.Should().Be(Error.None);
    }

    [Fact]
    public void Failure_carries_the_error()
    {
        var error = Error.Validation("bad input");

        Result result = error; // implicit conversion

        result.IsFailure.Should().BeTrue();
        result.Error.Should().Be(error);
    }

    [Fact]
    public void Generic_success_exposes_the_value()
    {
        Result<int> result = 42; // implicit conversion

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(42);
    }

    [Fact]
    public void Accessing_the_value_of_a_failure_throws()
    {
        Result<int> result = Error.NotFound("missing");

        var act = () => result.Value;

        act.Should().Throw<InvalidOperationException>();
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

        error.Code.Should().Be(expectedCode);
    }
}
