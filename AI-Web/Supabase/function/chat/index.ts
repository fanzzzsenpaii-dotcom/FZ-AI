import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const GEMINI_API_KEY =
  Deno.env.get("GEMINI_API_KEY");

const ALLOWED_MODELS = new Set([
  "gemini-2.5-flash",
  "gemini-2.5-pro",
  "gemini-2.5-flash-lite"
]);

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods":
    "POST, OPTIONS"
};


function json(
  data: unknown,
  status = 200
) {

  return new Response(
    JSON.stringify(data),
    {
      status,

      headers: {
        ...corsHeaders,
        "Content-Type":
          "application/json"
      }
    }
  );
}


Deno.serve(
  async (req: Request) => {

    if (
      req.method === "OPTIONS"
    ) {

      return new Response(
        "ok",
        {
          headers:
            corsHeaders
        }
      );

    }


    if (
      req.method !== "POST"
    ) {

      return json(
        {
          error:
            "Method tidak didukung."
        },
        405
      );

    }


    if (!GEMINI_API_KEY) {

      return json(
        {
          error:
            "GEMINI_API_KEY belum dikonfigurasi di Supabase."
        },
        500
      );

    }


    try {

      const body =
        await req.json();


      const model =
        body.model ||
        "gemini-2.5-flash";


      if (
        !ALLOWED_MODELS.has(model)
      ) {

        return json(
          {
            error:
              "Model tidak diizinkan."
          },
          400
        );

      }


      const inputMessages =
        Array.isArray(
          body.messages
        )
          ? body.messages
          : [];


      if (
        inputMessages.length === 0
      ) {

        return json(
          {
            error:
              "Pesan kosong."
          },
          400
        );

      }


      const contents =
        inputMessages
          .slice(-30)
          .map(
            (
              message: {
                role?: string;
                content?: string;
              }
            ) => ({

              role:
                message.role === "model"
                  ? "model"
                  : "user",

              parts: [
                {
                  text:
                    String(
                      message.content || ""
                    ).slice(
                      0,
                      12000
                    )
                }
              ]

            })
          );


      const endpoint =
        `https://generativelanguage.googleapis.com/v1beta/models/${model}:streamGenerateContent?alt=sse&key=${encodeURIComponent(
          GEMINI_API_KEY
        )}`;


      const geminiResponse =
        await fetch(
          endpoint,
          {
            method: "POST",

            headers: {
              "Content-Type":
                "application/json"
            },

            body: JSON.stringify({

              systemInstruction: {
                parts: [
                  {
                    text:
                      `Kamu adalah FZ AI, asisten AI yang membantu pengguna secara jelas, akurat, dan praktis.

Jika pengguna meminta kode, gunakan Markdown fenced code block dengan bahasa yang sesuai.

Jangan mengaku memiliki kemampuan yang tidak kamu miliki.

Jawab dalam bahasa pengguna.`
                  }
                ]
              },

              contents,

              generationConfig: {

                temperature: 0.7,

                maxOutputTokens:
                  4096

              }

            })

          }
        );


      if (
        !geminiResponse.ok
      ) {

        const errorText =
          await geminiResponse.text();

        console.error(
          "Gemini error:",
          errorText
        );

        return json(
          {
            error:
              "Gemini API gagal memproses request."
          },
          geminiResponse.status
        );

      }


      if (
        !geminiResponse.body
      ) {

        return json(
          {
            error:
              "Gemini tidak menyediakan stream."
          },
          502
        );

      }


      const upstream =
        geminiResponse.body;


      const decoder =
        new TextDecoder();

      const encoder =
        new TextEncoder();


      const stream =
        new ReadableStream({

          async start(
            controller
          ) {

            const reader =
              upstream.getReader();


            let buffer = "";


            try {

              while (true) {

                const {
                  value,
                  done
                } =
                  await reader.read();


                if (done) {
                  break;
                }


                buffer +=
                  decoder.decode(
                    value,
                    {
                      stream: true
                    }
                  );


                const lines =
                  buffer.split("\n");


                buffer =
                  lines.pop() || "";


                for (
                  const line
                  of lines
                ) {

                  const trimmed =
                    line.trim();


                  if (
                    !trimmed ||
                    !trimmed.startsWith(
                      "data:"
                    )
                  ) {
                    continue;
                  }


                  const raw =
                    trimmed
                      .slice(5)
                      .trim();


                  if (!raw) {
                    continue;
                  }


                  try {

                    const data =
                      JSON.parse(raw);


                    const parts =
                      data?.candidates?.[0]
                        ?.content
                        ?.parts;


                    if (
                      !Array.isArray(parts)
                    ) {
                      continue;
                    }


                    for (
                      const part
                      of parts
                    ) {

                      if (
                        typeof part.text !==
                        "string"
                      ) {
                        continue;
                      }


                      controller.enqueue(
                        encoder.encode(
                          `data: ${JSON.stringify({
                            text: part.text
                          })}\n\n`
                        )
                      );

                    }

                  } catch {

                    /*
                      Chunk SSE yang belum lengkap
                      akan diproses pada iterasi berikutnya.
                    */

                  }

                }

              }


              controller.enqueue(
                encoder.encode(
                  "data: [DONE]\n\n"
                )
              );


              controller.close();

            } catch (error) {

              console.error(
                "Stream error:",
                error
              );

              controller.error(
                error
              );

            } finally {

              reader.releaseLock();

            }

          }

        });


      return new Response(
        stream,
        {
          headers: {
            ...corsHeaders,

            "Content-Type":
              "text/event-stream",

            "Cache-Control":
              "no-cache",

            "Connection":
              "keep-alive",

            "X-Accel-Buffering":
              "no"
          }
        }
      );


    } catch (error) {

      console.error(
        "Function error:",
        error
      );

      return json(
        {
          error:
            "Terjadi kesalahan pada server."
        },
        500
      );

    }

  }
);
