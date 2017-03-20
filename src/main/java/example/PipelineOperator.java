package example;

public interface PipelineOperator<IT, OT> {
    OT operate(IT value);
}
