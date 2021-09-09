use arcon::prelude::*;

#[derive(prost::Message, Arcon, Copy, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
pub struct InputEvent {
    #[prost(int32, tag = "1")]
    pub value: i32,
}

#[derive(prost::Message, Arcon, Copy, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
pub struct OutputEvent {
    #[prost(int32, tag = "1")]
    pub value: i32,
}

#[derive(Default)]
pub struct SumOperator {
    sum: i32,
    count: i32,
}

impl Operator for SumOperator {
    type IN = InputEvent;
    type OUT = OutputEvent;
    type TimerState = ArconNever;
    type OperatorState = EmptyState;
    type ElementIterator = std::iter::Once<ArconElement<Self::OUT>>;

    fn handle_element(
        &mut self,
        element: ArconElement<Self::IN>,
        ctx: &mut OperatorContext<Self::TimerState, Self::OperatorState>,
    ) -> ArconResult<Self::ElementIterator> {
        self.sum += element.data.value;
        self.count += 1;
        let average = self.sum / self.count;
        info!(
            ctx.log(),
            "Current sum/count/avg: {}/{}/{}", self.sum, self.count, average
        );
        Ok(std::iter::once(ArconElement::new(OutputEvent {
            value: average,
        })))
    }

    arcon::ignore_timeout!();
}

fn main() {
    let data = (0i32..).map(|value| InputEvent { value });
    let mut app = Application::default()
        .iterator(data, |conf| {
            conf.set_timestamp_extractor(|x| x.value as u64)
        })
        .operator(OperatorBuilder {
            operator: Arc::new(|| SumOperator::default()),
            state: Arc::new(|_| EmptyState),
            conf: Default::default(),
        })
        .build();

    app.start();
    app.await_termination();
}
