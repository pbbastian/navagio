use crossbeam::queue::SegQueue;
use log::info;
use navagio::job::{self, JobGraph};
use simple_logger::SimpleLogger;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};
use winit::{event::Event, event_loop::EventLoop, window::WindowBuilder};
use winit_input_helper::WinitInputHelper;

fn main() {
    SimpleLogger::new().init().unwrap();
    let event_loop = EventLoop::new();

    let _window = WindowBuilder::new()
        .with_title("Navagio")
        .build(&event_loop)
        .unwrap();

    let proxy = event_loop.create_proxy();
    let event_queue = Arc::new(SegQueue::<Event<()>>::new());
    let event_count = Arc::new(AtomicUsize::new(0));

    {
        let mut job_graph = JobGraph::new();
        let input = job_graph.add_resource("Input", WinitInputHelper::new());

        let event_queue = event_queue.clone();
        let event_count = event_count.clone();
        job_graph
            .add_job("Poll Events")
            .with_mut(input)
            .schedule(move |input| {
                let event = Event::<()>::NewEvents(winit::event::StartCause::Poll);
                input.update(&event);

                let count = event_count.swap(0, Ordering::SeqCst);
                if count > 0 {
                    for _ in 0..count {
                        let event = event_queue.pop().unwrap();
                        input.update(&event);
                    }
                }

                let event: Event<()> = Event::MainEventsCleared;
                input.update(&event);
            });

        job_graph
            .add_job("Event Handling")
            .with_ref(input)
            .schedule(move |input| {
                if input.key_pressed(winit::event::VirtualKeyCode::Q) {
                    info!("Q was pressed!");
                }

                if input.quit() {
                    job::abort();
                    proxy.send_event(()).unwrap();
                }
            });

        // TODO: JobGraph should return a join handle instead
        thread::spawn(move || job_graph.run());
    }

    let event_queue = event_queue.clone();
    let event_count = event_count.clone();
    let mut current_event_count: usize = 0;
    event_loop.run(move |event, _, control_flow| {
        *control_flow = winit::event_loop::ControlFlow::Wait;

        match event {
            Event::UserEvent(_) => *control_flow = winit::event_loop::ControlFlow::Exit,
            Event::NewEvents(_) => (),
            Event::MainEventsCleared => {
                event_count.fetch_add(current_event_count, Ordering::SeqCst);
                current_event_count = 0;
            }
            _ => {
                event_queue.push(unsafe { std::mem::transmute(event) });
                current_event_count += 1;
            }
        }
    });
}
