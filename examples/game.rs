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
use winit::{event::Event, event::WindowEvent, event_loop::EventLoop, window::WindowBuilder};
use winit_input_helper::WinitInputHelper;

fn main() {
    SimpleLogger::new().init().unwrap();
    let event_loop = EventLoop::new();

    let window = WindowBuilder::new()
        .with_title("Navagio")
        .build(&event_loop)
        .unwrap();

    let event_queue = Arc::new(SegQueue::<Event<()>>::new());
    let event_count = Arc::new(AtomicUsize::new(0));

    {
        let mut event_queue = event_queue.clone();
        let mut event_count = event_count.clone();
        let mut input = WinitInputHelper::new();
        // TODO: JobGraph should return a join handle instead
        thread::spawn(move || {
            let mut job_graph = JobGraph::new();
            let event_queue = job_graph.add_resource("Event Queue", &mut event_queue);
            let event_count = job_graph.add_resource("Event Count", &mut event_count);
            let input = job_graph.add_resource("Input", &mut input);

            let mut did_update_last_frame = false;
            job_graph
                .add_job("Poll Events")
                .with_ref(event_queue)
                .with_ref(event_count)
                .with_mut(input)
                .schedule(move |event_queue, event_count, input| {
                    let count = event_count.swap(0, Ordering::SeqCst);
                    if count > 0 {
                        for _ in 0..count {
                            let event = event_queue.pop().unwrap();
                            input.update(&event);
                        }
                        did_update_last_frame = true;
                    } else if did_update_last_frame {
                        // Fake events to clear state so that we don't get repeated inputs.
                        let event: Event<()> = Event::NewEvents(winit::event::StartCause::Poll);
                        input.update(&event);
                        let event: Event<()> = Event::MainEventsCleared;
                        input.update(&event);
                        did_update_last_frame = false;
                    }
                });

            job_graph.add_job("If Q").with_ref(input).schedule(|input| {
                if input.key_pressed(winit::event::VirtualKeyCode::Q) {
                    info!("Q was pressed!");
                }
            });

            job_graph.run();
        });
    }

    let event_queue = event_queue.clone();
    let event_count = event_count.clone();
    let mut current_event_count: usize = 0;
    event_loop.run(move |event, _, control_flow| {
        *control_flow = winit::event_loop::ControlFlow::Wait;

        let mut main_events_cleared = false;
        match event {
            Event::WindowEvent {
                event: WindowEvent::CloseRequested,
                window_id,
            } if window_id == window.id() => {
                *control_flow = winit::event_loop::ControlFlow::Exit;
                job::abort();
            }
            Event::MainEventsCleared => main_events_cleared = true,
            _ => (),
        }

        event_queue.push(unsafe { std::mem::transmute(event) });
        current_event_count += 1;

        if main_events_cleared {
            event_count.fetch_add(current_event_count, Ordering::SeqCst);
            current_event_count = 0;
        }
    });
}
