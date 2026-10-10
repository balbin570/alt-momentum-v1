 "get":  {
                "summary":  "V90 Exit Study",
                "operationId":  "v90_exit_study_v90_exit_study_get",
                "parameters":  [
                                   {
                                       "name":  "limit",
                                       "in":  "query",
                                       "required":  false,
                                       "schema":  {
                                                      "type":  "integer",
                                                      "maximum":  200,
                                                      "minimum":  1,
                                                      "default":  100,
                                                      "title":  "Limit"
                                                  }
                                   }
                               ],
                "responses":  {
                                  "200":  {
                                              "description":  "Successful Response",
                                              "content":  {
                                                              "application/json":  {
                                                                                       "schema":  {

                                                                                                  }
                                                                                   }
                                                          }
                                          },
                                  "422":  {
                                              "description":  "Validation Error",
                                              "content":  {
                                                              "application/json":  {
                                                                                       "schema":  {
                                                                                                      "$ref":  "#/components/schemas/HTTPValidationError"
                                                                                                  }
                                                                                   }
                                                          }
                                          }
                              }
            }
