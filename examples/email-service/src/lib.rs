use apalis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Email {
    pub to: String,
    pub subject: String,
    pub text: String,
}

impl Job for Email {
    const NAME: &'static str = "apalis::Email";
}

pub async fn send_email(job: Email) {
    log::info!("Attempting to send email to {}", job.to);
}

pub const FORM_HTML: &str = r#"
        <!doctype html>
        <html>
            <head>
                <link href="https://unpkg.com/tailwindcss@1.2.0/dist/tailwind.min.css" rel="stylesheet">
                <meta credits="https://tailwindcomponents.com/component/basic-contact-form" />
            </head>
            <body>
                <form style="margin: 0 auto;" class="w-full max-w-lg pt-20" action="/" method="post">
                    <div class="flex flex-wrap -mx-3 mb-6">
                    <div class="w-full md:w-2/3 px-3 mb-6 md:mb-0">
                        <label class="block uppercase tracking-wide text-gray-700 text-xs font-bold mb-2" for="to">
                        To
                        </label>
                        <input class="appearance-none block w-full bg-gray-200 text-gray-700 border border-red-500 rounded py-3 px-4 mb-3 leading-tight focus:outline-none focus:bg-white" id="to" type="email" name="to" placeholder="test@example.com">
                        <p class="text-red-500 text-xs italic">Please fill out this field.</p>
                    </div>

                    </div>
                    <div class="flex flex-wrap -mx-3 mb-6">
                    <div class="w-full px-3">
                        <label class="block uppercase tracking-wide text-gray-700 text-xs font-bold mb-2" for="subject">
                        Subject
                        </label>
                        <input class="appearance-none block w-full bg-gray-200 text-gray-700 border border-gray-200 rounded py-3 px-4 mb-3 leading-tight focus:outline-none focus:bg-white focus:border-gray-500" id="subject" type="text" name="subject">
                        <p class="text-gray-600 text-xs italic">Some tips - as long as needed</p>
                    </div>
                    </div>
                    <div class="flex flex-wrap -mx-3 mb-6">
                    <div class="w-full px-3">
                        <label class="block uppercase tracking-wide text-gray-700 text-xs font-bold mb-2" for="text">
                        Message
                        </label>
                        <textarea class=" no-resize appearance-none block w-full bg-gray-200 text-gray-700 border border-gray-200 rounded py-3 px-4 mb-3 leading-tight focus:outline-none focus:bg-white focus:border-gray-500 h-48 resize-none" id="text" name="text" ></textarea>
                    </div>
                    </div>
                    <div class="md:flex md:items-center">
                    <div class="md:w-1/3">
                        <button class="shadow bg-teal-400 hover:bg-teal-400 focus:shadow-outline focus:outline-none text-white font-bold py-2 px-4 rounded" type="submit">
                        Send
                        </button>
                    </div>
                    <div class="md:w-2/3"></div>
                    </div>
                </form>
            </body>
        </html>
        "#;

#[derive(Debug)]
pub enum EmailError {
    NoStorage,
    SomeError(&'static str),
}

impl std::fmt::Display for EmailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
